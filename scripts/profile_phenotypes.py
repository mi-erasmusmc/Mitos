#!/usr/bin/env python

from __future__ import annotations

import argparse
import json
import time
import tempfile
from pathlib import Path
import uuid

import ibis

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - optional dependency checked at runtime.
    duckdb = None

from mitos.cohort_expression import CohortExpression
from mitos.build_context import CohortBuildOptions, BuildContext, compile_codesets
from mitos.builders.pipeline import build_primary_events
from compare_cohort_counts import (
    generate_circe_sql_via_r,
    quote_ident,
    qualify_identifier,
    _split_sql_statements,
)


def _quote_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _profile_statement(conn, statement: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    escaped = _quote_path(output_path)
    conn.raw_sql("PRAGMA enable_profiling='json'")
    conn.raw_sql(f"PRAGMA profiling_output='{escaped}'")
    try:
        conn.raw_sql(statement)
    finally:
        conn.raw_sql("PRAGMA disable_profiling")


def profile_ibis_sql(conn, sql: str, output_path: Path) -> None:
    """
    Profile a single Ibis SQL statement and write the plan to output_path.
    Wraps the query in a CREATE TEMP TABLE to ensure full execution.
    """
    sink_name = f"_ibis_profile_sink_{uuid.uuid4().hex[:8]}"
    # Ibis generates a single query for the primary event table.
    # We wrap it to force materialization/execution.
    statement = f"CREATE TEMP TABLE {sink_name} AS {sql}"
    try:
        _profile_statement(conn, statement, output_path)
    finally:
        conn.raw_sql(f"DROP TABLE IF EXISTS {sink_name}")


def profile_circe_sql(
    sql_script: str,
    db_path: str,
    output_path: Path,
    *,
    result_schema: str,
    target_table: str,
    cohort_id: int,
    temp_schema: str | None = None,
):
    """
    Execute the Circe SQL script step-by-step, profiling each statement.
    Aggregates all profile JSONs into a single list at output_path.
    """
    if duckdb is None:
        raise RuntimeError(
            "duckdb Python package is required for Circe profiling. Install it via `pip install duckdb`."
        )
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    statements = _split_sql_statements(sql_script)
    
    conn = duckdb.connect(database=db_path, read_only=False)
    qualified_table = qualify_identifier(target_table, result_schema)
    
    # Setup schemas/tables
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

    aggregated_plans = []
    
    # Use a temporary directory to store individual step profiles
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        conn.execute("BEGIN TRANSACTION;")
        try:
            for i, stmt in enumerate(statements):
                if not stmt.strip():
                    continue
                
                step_profile = tmp_path / f"step_{i:03d}.json"
                escaped = _quote_path(step_profile)
                
                # Enable profiling for this specific statement
                conn.execute("PRAGMA enable_profiling='json'")
                conn.execute(f"PRAGMA profiling_output='{escaped}'")
                
                try:
                    conn.execute(stmt)
                except Exception as e:
                    print(f"Error executing Circe statement {i}: {e}")
                    raise
                finally:
                    conn.execute("PRAGMA disable_profiling")
                
                # Read the generated profile and add to list
                if step_profile.exists():
                    try:
                        data = json.loads(step_profile.read_text())
                        # Annotate which statement this was
                        data["sql_statement"] = stmt
                        aggregated_plans.append(data)
                    except json.JSONDecodeError:
                        pass
        finally:
            conn.execute("ROLLBACK;")
            conn.close()

    # Write the aggregated plan
    output_path.write_text(json.dumps(aggregated_plans, indent=2))


def profile_expression(
    conn,
    expression: CohortExpression,
    options: CohortBuildOptions,
    *,
    profile_path: Path | None = None,
):
    compile_start = time.perf_counter()
    resource = compile_codesets(conn, expression.concept_sets, options)
    compile_ms = (time.perf_counter() - compile_start) * 1000

    ctx = BuildContext(conn, options, resource)
    try:
        build_start = time.perf_counter()
        events = build_primary_events(expression, ctx)
        build_ms = (time.perf_counter() - build_start) * 1000
        if events is None:
            return {
                "compile_ms": compile_ms,
                "build_ms": build_ms,
                "sql_lines": 0,
                "plan_lines": 0,
                "row_count": 0,
            }
        sql = events.compile().rstrip(";")
        sql_chars = len(sql)
        if profile_path is not None:
            profile_ibis_sql(conn, sql, profile_path)
        
        plan_rows = conn.raw_sql(f"EXPLAIN {sql}").fetchall()
        plan_text = "\n".join(
            str(row[1]) if isinstance(row, tuple) and len(row) > 1 else str(row[0])
            for row in plan_rows
        )
        plan_lines = len(plan_text.splitlines()) or 1
        row_count = None
        count_ms = None
        if options.generate_stats:
            count_start = time.perf_counter()
            row_count = conn.raw_sql(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
            count_ms = (time.perf_counter() - count_start) * 1000
        return {
            "compile_ms": compile_ms,
            "build_ms": build_ms,
            "sql_chars": sql_chars,
            "plan_lines": plan_lines,
            "row_count": row_count,
            "count_ms": count_ms,
        }
    finally:
        ctx.close()


def main():
    parser = argparse.ArgumentParser(description="Profile phenotype SQL size and plan complexity.")
    parser.add_argument(
        "--db",
        default="duckyS_local.duckdb",
        help="DuckDB database path",
    )
    parser.add_argument(
        "--cdm-schema",
        default="main",
        help="CDM schema name inside the DuckDB database.",
    )
    parser.add_argument(
        "--vocab-schema",
        help="Vocabulary schema name (defaults to the CDM schema when omitted).",
    )
    parser.add_argument(
        "--result-schema",
        help="Schema that will hold Circe target tables (defaults to the CDM schema).",
    )
    parser.add_argument(
        "--target-table",
        default="circe_cohort",
        help="Target cohort table for Circe SQL profiling.",
    )
    parser.add_argument(
        "--cohort-id",
        type=int,
        default=1,
        help="Cohort definition id used for Circe profiling.",
    )
    parser.add_argument(
        "--temp-schema",
        default="scratch",
        help="Schema used to emulate temporary tables for Circe SQL.",
    )
    parser.add_argument(
        "--fixtures",
        default="cohorts/phenotypes",
        help="Directory containing phenotype JSON files",
    )
    parser.add_argument(
        "--phenotypes",
        nargs="*",
        default=["phenotype-2.json", "phenotype-30.json", "phenotype-78.json", "phenotype-344.json", "phenotype-500.json"],
        help="Phenotype JSON filenames relative to fixtures directory",
    )
    parser.add_argument("--output", help="Optional path to write JSON results")
    parser.add_argument(
        "--row-count",
        action="store_true",
        help="Execute cohort SQL to compute row counts (can be slow)",
    )
    parser.add_argument(
        "--profile-dir",
        default="profiles",
        help="Directory to write DuckDB JSON profiling artifacts.",
    )
    parser.add_argument(
        "--skip-circe",
        action="store_true",
        help="Skip Circe SQL generation and profiling.",
    )
    args = parser.parse_args()

    fixtures_dir = Path(args.fixtures)
    profile_dir = Path(args.profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)
    result_schema = args.result_schema or args.cdm_schema
    temp_schema = args.temp_schema or result_schema
    vocab_schema = args.vocab_schema or args.cdm_schema
    results = []
    
    for name in args.phenotypes:
        path = fixtures_dir / name
        if not path.exists():
            print(f"Skipping missing phenotype {path}")
            continue
        expression = CohortExpression.model_validate_json(path.read_text())
        conn = ibis.duckdb.connect(database=args.db)
        
        phenotype_dir = profile_dir / path.stem
        phenotype_dir.mkdir(parents=True, exist_ok=True)
        
        # Output paths requested by user
        ibis_plan_path = phenotype_dir / "ibis_plan.json"
        
        try:
            options = CohortBuildOptions(
                cdm_schema=args.cdm_schema,
                vocabulary_schema=vocab_schema,
                result_schema=result_schema,
                target_table=args.target_table,
                cohort_id=args.cohort_id,
                generate_stats=args.row_count,
                temp_emulation_schema=temp_schema,
            )
            metrics = profile_expression(conn, expression, options, profile_path=ibis_plan_path)
        finally:
            if hasattr(conn, "close"):
                conn.close()
        metrics["ibis_profile"] = str(ibis_plan_path)

        circe_plan_path = None
        circe_generate_ms = None
        if not args.skip_circe:
            circe_plan_path = phenotype_dir / "circe_plan.json"
            circe_sql, circe_generate_ms = generate_circe_sql_via_r(
                json_path=path,
                cdm_schema=args.cdm_schema,
                vocab_schema=vocab_schema,
                result_schema=result_schema,
                target_schema=result_schema,
                target_table=args.target_table,
                cohort_id=args.cohort_id,
                temp_schema=temp_schema,
            )
            profile_circe_sql(
                circe_sql,
                args.db,
                circe_plan_path,
                result_schema=result_schema,
                target_table=args.target_table,
                cohort_id=args.cohort_id,
                temp_schema=temp_schema,
            )
        metrics["circe_profile"] = str(circe_plan_path) if circe_plan_path else None
        metrics["circe_generate_ms"] = circe_generate_ms
        results.append({"phenotype": name, **metrics})
        print(
            f"{name}: sql_chars={metrics['sql_chars']} plan_lines={metrics['plan_lines']} rows={metrics['row_count']} "
            f"ibis_profile={metrics['ibis_profile']} "
            f"circe_profile={metrics['circe_profile']}"
        )

    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
