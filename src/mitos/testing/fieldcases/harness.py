from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import ibis
import polars as pl

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events
from mitos.cohort_expression import CohortExpression
from mitos.testing.circe_oracle import CirceSqlConfig, execute_circe_sql, generate_circe_sql_via_r
from mitos.testing.omop.builder import OmopBuilder
from mitos.testing.omop.vocab import build_minimal_vocab


@dataclass(frozen=True)
class FieldCase:
    name: str
    cohort_json: dict
    build_omop: Callable[[OmopBuilder], None]
    cohort_id: int = 1


def _ensure_empty_cohort_table(con: ibis.BaseBackend, *, schema: str, name: str) -> None:
    con.raw_sql(
        f"""
CREATE TABLE IF NOT EXISTS {schema}.{name} (
  cohort_definition_id BIGINT,
  subject_id BIGINT,
  cohort_start_date DATE,
  cohort_end_date DATE
)
""".strip()
    )


def _read_cohort_rows(con: ibis.BaseBackend, *, schema: str, name: str, cohort_id: int) -> pl.DataFrame:
    # Avoid Ibis -> DuckDB relation conversion (to_pyarrow) which is relatively expensive;
    # use the native DuckDB connection to fetch Arrow/Polars directly.
    duck = con.con
    rel = duck.execute(
        f"""
SELECT subject_id, cohort_start_date, cohort_end_date
FROM {schema}.{name}
WHERE cohort_definition_id = ?
""".strip(),
        [int(cohort_id)],
    )
    df = pl.from_arrow(rel.fetch_arrow_table())
    if df.is_empty():
        return df
    return df.unique().sort(["subject_id", "cohort_start_date", "cohort_end_date"])

def _cohort_rows_from_events(
    con: ibis.BaseBackend, events: ibis.expr.types.Table, *, cohort_id: int | None
) -> pl.DataFrame:
    cohort_id_expr = (
        ibis.literal(int(cohort_id), type="int64")
        if cohort_id is not None
        else ibis.null().cast("int64")
    )
    df = (
        events.select(
            cohort_id_expr.name("cohort_definition_id"),
            events.person_id.cast("int64").name("subject_id"),
            events.start_date.cast("date").name("cohort_start_date"),
            events.end_date.cast("date").name("cohort_end_date"),
        )
    )
    sql = con.compile(df)
    duck = con.con
    out = pl.from_arrow(duck.execute(sql).fetch_arrow_table())
    if out.is_empty():
        return out
    return out.select(["subject_id", "cohort_start_date", "cohort_end_date"]).unique().sort(
        ["subject_id", "cohort_start_date", "cohort_end_date"]
    )

def run_fieldcase(case: FieldCase, *, circe_sql: str | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Execute one FieldCase against:
      - Circe SQL (via R/CirceR) as oracle
      - Mitos Python builders
    and return (circe_rows, python_rows).
    """
    tmp_dir: Path | None = None
    json_path: Path | None = None
    if circe_sql is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix=f"mitos_fieldcase_{case.name}_"))
        json_path = tmp_dir / "cohort.json"
        json_path.write_text(json.dumps(case.cohort_json, indent=2) + "\n", encoding="utf-8")

    try:
        con = ibis.duckdb.connect(database=":memory:")
        schema = "main"
        cohort_table = "_cohort_rows"
        circe_table = "_circe_cohort_rows"

        expr = CohortExpression.model_validate(case.cohort_json)

        vocab = build_minimal_vocab(expr.concept_sets)
        builder = OmopBuilder(schema=schema)
        case.build_omop(builder)
        builder.materialize(con, ensure_all_tables=False, fast=True)

        # Fast-path load vocab tables, too (avoid Ibis create_table overhead).
        duck = con.con
        for name, df in [
            ("concept", vocab.concept),
            ("concept_ancestor", vocab.concept_ancestor),
            ("concept_relationship", vocab.concept_relationship),
        ]:
            tmp = f"__mitos_vocab_{name}__"
            con.raw_sql(f"DROP TABLE IF EXISTS {schema}.{name}")
            duck.register(tmp, df.to_arrow())
            try:
                con.raw_sql(f"CREATE TABLE {schema}.{name} AS SELECT * FROM {tmp}")
            finally:
                duck.unregister(tmp)

        _ensure_empty_cohort_table(con, schema=schema, name=cohort_table)
        _ensure_empty_cohort_table(con, schema=schema, name=circe_table)

        # Circe execution (oracle)
        if circe_sql is None:
            assert json_path is not None
            circe_sql, _ = generate_circe_sql_via_r(
                CirceSqlConfig(
                    json_path=json_path,
                    cdm_schema=schema,
                    vocab_schema=schema,
                    result_schema=schema,
                    target_schema=schema,
                    target_table=circe_table,
                    cohort_id=case.cohort_id,
                    target_dialect="duckdb",
                )
            )
        execute_circe_sql(con, circe_sql)

        # Python execution
        options = CohortBuildOptions(
            cdm_schema=schema,
            vocabulary_schema=schema,
            result_schema=schema,
            target_table=cohort_table,
            cohort_id=case.cohort_id,
            backend="duckdb",
            materialize_stages=False,
            materialize_codesets=False,
            generate_stats=False,
        )
        codesets = compile_codesets(con, expr.concept_sets, options)
        ctx = BuildContext(con, options, codesets)
        try:
            events = build_primary_events(expr, ctx)
            if events is None:
                # Ensure table exists even when expression yields none.
                python_rows = pl.DataFrame(
                    schema={
                        "subject_id": pl.Int64,
                        "cohort_start_date": pl.Date,
                        "cohort_end_date": pl.Date,
                    }
                )
            else:
                python_rows = _cohort_rows_from_events(con, events, cohort_id=case.cohort_id)
        finally:
            ctx.close()

        circe_rows = _read_cohort_rows(con, schema=schema, name=circe_table, cohort_id=case.cohort_id)
        return circe_rows, python_rows
    finally:
        if tmp_dir is not None:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def require_non_empty(df: pl.DataFrame, *, case_name: str, engine: str) -> None:
    if df.is_empty():
        raise AssertionError(
            f"FieldCase {case_name}: {engine} returned 0 rows; discriminator case is invalid"
        )


def assert_same_rows(circe_rows: pl.DataFrame, python_rows: pl.DataFrame) -> None:
    if circe_rows.is_empty() and python_rows.is_empty():
        return
    circe_set = set(
        map(
            tuple,
            circe_rows.select(["subject_id", "cohort_start_date", "cohort_end_date"]).to_numpy(),
        )
    )
    py_set = set(
        map(
            tuple,
            python_rows.select(["subject_id", "cohort_start_date", "cohort_end_date"]).to_numpy(),
        )
    )
    if circe_set != py_set:
        missing_in_python = sorted(circe_set - py_set)
        missing_in_circe = sorted(py_set - circe_set)
        raise AssertionError(
            "Cohort row mismatch.\n"
            f"missing_in_python={len(missing_in_python)} missing_in_circe={len(missing_in_circe)}\n"
            f"examples_missing_in_python={missing_in_python[:10]}\n"
            f"examples_missing_in_circe={missing_in_circe[:10]}\n"
        )


def rscript_available() -> bool:
    return shutil.which("Rscript") is not None
