#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
import textwrap
from pathlib import Path
import time

import ibis

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - optional dependency is validated at runtime.
    duckdb = None

from ibis_cohort.build_context import BuildContext, CohortBuildOptions, compile_codesets
from ibis_cohort.builders.pipeline import build_primary_events
from ibis_cohort.cohort_expression import CohortExpression


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Python/Ibis and Circe-generated cohort row counts for a single JSON."
    )
    parser.add_argument(
        "--json",
        default="cohorts/6243-dementia-outcome-v1.json",
        help="Path to the cohort expression JSON file.",
    )
    parser.add_argument(
        "--cdm-db",
        default="/home/egill/database/database-1M_filtered.duckdb",
        help="DuckDB database path that contains the OMOP CDM.",
    )
    parser.add_argument("--cdm-schema", default="main", help="CDM schema name inside the DuckDB database.")
    parser.add_argument(
        "--vocab-schema",
        help="Vocabulary schema name (defaults to the CDM schema when omitted).",
    )
    parser.add_argument(
        "--result-schema",
        help="Schema that holds the cohort target table (defaults to the CDM schema).",
    )
    parser.add_argument(
        "--target-table",
        default="circe_cohort",
        help="Fully writable cohort table that Circe SQL will insert into.",
    )
    parser.add_argument(
        "--cohort-id",
        type=int,
        default=1,
        help="Cohort definition id used for the Circe target table.",
    )
    parser.add_argument(
        "--temp-schema",
        default="scratch",
        help="Schema used to emulate temporary tables for Circe SQL.",
    )
    parser.add_argument(
        "--python-sql-out",
        help="Optional path to save the Python-generated SQL.",
    )
    parser.add_argument(
        "--circe-sql-out",
        help="Optional path to save the Circe-generated SQL.",
    )
    return parser.parse_args()


def quote_ident(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def qualify_identifier(name: str, schema: str | None) -> str:
    if schema:
        return f"{quote_ident(schema)}.{quote_ident(name)}"
    return quote_ident(name)


def wrap_count_query(sql: str) -> str:
    trimmed = sql.strip().rstrip(";")
    return f"SELECT COUNT(*) AS row_count FROM ({trimmed}) as cohort_rows"


def run_python_pipeline(
    json_path: Path,
    db_path: str,
    cdm_schema: str,
    vocab_schema: str | None,
) -> tuple[str, int, dict[str, float]]:
    expression = CohortExpression.model_validate_json(json_path.read_text())
    conn = ibis.duckdb.connect(database=db_path)
    schema = vocab_schema or cdm_schema
    options = CohortBuildOptions(cdm_schema=cdm_schema, vocabulary_schema=schema)
    compile_start = time.perf_counter()
    resource = compile_codesets(conn, expression.concept_sets, options)
    compile_ms = (time.perf_counter() - compile_start) * 1000
    ctx = BuildContext(conn, options, resource)
    try:
        build_start = time.perf_counter()
        events = build_primary_events(expression, ctx)
        build_ms = (time.perf_counter() - build_start) * 1000
        if events is None:
            raise RuntimeError("Cohort expression did not produce any primary events.")
        compile_sql_start = time.perf_counter()
        sql = events.compile()
        compile_sql_ms = (time.perf_counter() - compile_sql_start) * 1000
        count_sql = wrap_count_query(sql)
        count_start = time.perf_counter()
        count = int(conn.raw_sql(count_sql).fetchone()[0])
        sql_exec_ms = (time.perf_counter() - count_start) * 1000
    finally:
        ctx.close()
        if hasattr(conn, "close"):
            conn.close()
    metrics = {
        "codeset_compile_ms": compile_ms,
        "build_ms": build_ms,
        "sql_compile_ms": compile_sql_ms,
        "sql_exec_ms": sql_exec_ms,
    }
    return sql, count, metrics


def generate_circe_sql_via_r(
    json_path: Path,
    cdm_schema: str,
    vocab_schema: str,
    result_schema: str,
    target_schema: str,
    target_table: str,
    cohort_id: int,
    temp_schema: str,
) -> tuple[str, float]:
    r_script = textwrap.dedent(
        """
        suppressPackageStartupMessages({
          library(CirceR)
          library(SqlRender)
        })
        args <- commandArgs(trailingOnly = TRUE)
        if (length(args) != 8) {
          stop("Expected 8 trailing arguments for JSON and schema metadata.")
        }
        json_path <- args[[1]]
        cdm_schema <- args[[2]]
        vocab_schema <- args[[3]]
        result_schema <- args[[4]]
        target_schema <- args[[5]]
        target_table <- args[[6]]
        cohort_id <- as.integer(args[[7]])
        temp_schema <- args[[8]]

        if (cdm_schema == "") stop("cdm_schema is required")
        if (vocab_schema == "") vocab_schema <- cdm_schema
        if (result_schema == "") result_schema <- cdm_schema
        if (target_schema == "") target_schema <- result_schema
        if (temp_schema == "") temp_schema <- target_schema

        json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\\n")
        expression <- CirceR::cohortExpressionFromJson(json_str)
        options <- CirceR::createGenerateOptions()
        options$generateStats <- FALSE
        options$useTempTables <- FALSE
        options$tempEmulationSchema <- temp_schema
        sql <- CirceR::buildCohortQuery(expression, options)
        sql <- SqlRender::render(
          sql,
          cdm_database_schema = cdm_schema,
          vocabulary_database_schema = vocab_schema,
          results_database_schema = result_schema,
          target_database_schema = target_schema,
          cohort_database_schema = target_schema,
          target_cohort_table = target_table,
          target_cohort_id = cohort_id,
          tempEmulationSchema = temp_schema
        )
        sql <- SqlRender::translate(sql = sql, targetDialect = "duckdb")
        cat(sql)
        """
    ).strip()

    cmd = [
        "Rscript",
        "-e",
        r_script,
        str(json_path),
        cdm_schema or "",
        vocab_schema or "",
        result_schema or "",
        target_schema or "",
        target_table,
        str(cohort_id),
        temp_schema or "",
    ]
    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_ms = (time.perf_counter() - start) * 1000
    if result.returncode != 0:
        raise RuntimeError(
            "Circe SQL generation failed:\n" f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout, elapsed_ms


def execute_circe_sql(
    sql: str,
    db_path: str,
    result_schema: str,
    target_table: str,
    cohort_id: int,
    temp_schema: str,
) -> tuple[int, dict[str, float]]:
    if duckdb is None:
        raise RuntimeError(
            "The 'duckdb' Python package is required to execute Circe SQL. Install it via `pip install duckdb`."
        )
    conn = duckdb.connect(database=db_path, read_only=False)
    qualified_table = qualify_identifier(target_table, result_schema)
    try:
        if result_schema:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(result_schema)};")
        if temp_schema:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(temp_schema)};")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                cohort_definition_id BIGINT,
                subject_id BIGINT,
                cohort_start_date DATE,
                cohort_end_date DATE
            );
            """
        )
        sql_start = time.perf_counter()
        conn.execute(sql)
        sql_exec_ms = (time.perf_counter() - sql_start) * 1000
        count_start = time.perf_counter()
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {qualified_table} WHERE cohort_definition_id = ?;",
            [cohort_id],
        ).fetchone()[0]
        count_query_ms = (time.perf_counter() - count_start) * 1000
        conn.execute(
            f"DELETE FROM {qualified_table} WHERE cohort_definition_id = ?;",
            [cohort_id],
        )
        metrics = {"sql_exec_ms": sql_exec_ms, "count_query_ms": count_query_ms}
        return int(row_count), metrics
    finally:
        conn.close()


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    if not json_path.exists():
        raise SystemExit(f"Cohort JSON not found: {json_path}")

    python_sql, python_count, python_metrics = run_python_pipeline(
        json_path=json_path,
        db_path=args.cdm_db,
        cdm_schema=args.cdm_schema,
        vocab_schema=args.vocab_schema,
    )
    if args.python_sql_out:
        Path(args.python_sql_out).write_text(python_sql)

    result_schema = args.result_schema or args.cdm_schema
    target_schema = result_schema

    circe_sql, circe_generate_ms = generate_circe_sql_via_r(
        json_path=json_path,
        cdm_schema=args.cdm_schema,
        vocab_schema=args.vocab_schema or args.cdm_schema,
        result_schema=result_schema,
        target_schema=target_schema,
        target_table=args.target_table,
        cohort_id=args.cohort_id,
        temp_schema=args.temp_schema or result_schema,
    )
    if args.circe_sql_out:
        Path(args.circe_sql_out).write_text(circe_sql)

    circe_count, circe_exec_metrics = execute_circe_sql(
        sql=circe_sql,
        db_path=args.cdm_db,
        result_schema=target_schema,
        target_table=args.target_table,
        cohort_id=args.cohort_id,
        temp_schema=args.temp_schema or result_schema,
    )
    circe_metrics = {
        "generate_ms": circe_generate_ms,
        **circe_exec_metrics,
    }

    print(
        "Python/Ibis row count: "
        f"{python_count} "
        f"(sql_exec={python_metrics['sql_exec_ms']:.1f}ms, "
        f"sql_compile={python_metrics['sql_compile_ms']:.1f}ms, "
        f"build={python_metrics['build_ms']:.1f}ms, "
        f"codesets={python_metrics['codeset_compile_ms']:.1f}ms)"
    )
    print(
        "Circe row count: "
        f"{circe_count} "
        f"(sql_exec={circe_metrics['sql_exec_ms']:.1f}ms, "
        f"generate={circe_metrics['generate_ms']:.1f}ms, "
        f"count_query={circe_metrics['count_query_ms']:.1f}ms)"
    )

    if python_count != circe_count:
        print("Row counts do not match.", file=sys.stderr)
        return 1

    print("Row counts match.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
