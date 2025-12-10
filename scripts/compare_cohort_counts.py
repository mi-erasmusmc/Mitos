#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import TypeAlias, cast

import yaml
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions, compile_codesets
from ibis_cohort.builders.pipeline import build_primary_events
from ibis_cohort.cohort_expression import CohortExpression

IBIS_TO_OHDSI_DIALECT = {
    "duckdb": "duckdb",
    "databricks": "spark",
    "pyspark": "spark",
    "postgres": "postgresql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "mssql": "sql server",
}

ConfigPrimitive: TypeAlias = str | int | float | bool | None
ConfigValue: TypeAlias = ConfigPrimitive | list["ConfigValue"] | dict[str, "ConfigValue"]
ConfigDict: TypeAlias = dict[str, ConfigValue]


def load_config_with_env_vars(config_path: str, profile_name: str) -> ConfigDict:
    if not os.path.exists(config_path):
        return {}

    with open(config_path, "r") as f:
        try:
            raw_config_obj = cast(
                dict[str, ConfigValue] | list[ConfigValue] | None, yaml.safe_load(f)
            )
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML config: {e}") from e

    if not isinstance(raw_config_obj, dict):
        raise ValueError(f"Config root must be a mapping, got {type(raw_config_obj)}")
    raw_mapping: dict[str, ConfigValue] = raw_config_obj

    if profile_name not in raw_mapping:
        raise ValueError(f"Profile '{profile_name}' not found in {config_path}")

    profile_raw = raw_mapping[profile_name]
    if not isinstance(profile_raw, dict):
        raise ValueError(
            f"Profile '{profile_name}' must be a mapping, got {type(profile_raw)}"
        )
    profile_config: ConfigDict = cast(ConfigDict, profile_raw)

    def substitute_value(item: ConfigValue) -> ConfigValue:
        if isinstance(item, dict):
            return {k: substitute_value(v) for k, v in item.items()}
        if isinstance(item, list):
            return [substitute_value(i) for i in item]
        if isinstance(item, str):
            matches: list[str] = re.findall(r"\$\{([^}]+)\}", item)
            for var_name in matches:
                env_val = os.environ.get(var_name)
                if env_val is None:
                    raise ValueError(f"Missing environment variable: {var_name}")
                item = item.replace(f"${{{var_name}}}", env_val)
            return item
        return item

    substituted = substitute_value(profile_config)
    return cast(ConfigDict, substituted)


def get_connection(args: argparse.Namespace) -> ibis.BaseBackend:
    config: ConfigDict = {}

    if args.config and args.profile:
        print(f"Loading profile '{args.profile}' from {args.config}...")
        config = load_config_with_env_vars(args.config, args.profile)

    if args.backend:
        config["backend"] = args.backend

    backend_value = config.pop("backend", "duckdb")
    if not isinstance(backend_value, str):
        raise ValueError(f"Backend must be a string, got {type(backend_value)}")
    backend_name = backend_value

    if backend_name == "duckdb" and args.cdm_db:
        config["database"] = args.cdm_db

    print(f"Connecting to backend: {backend_name}")
    try:
        entrypoint = getattr(ibis, backend_name)
        con = entrypoint.connect(**config)
        return con
    except AttributeError:
        raise ValueError(f"Ibis backend '{backend_name}' is not installed or invalid.")
    except Exception as e:
        raise RuntimeError(f"Failed to connect to {backend_name}: {e}")


def get_ohdsi_dialect(con: ibis.BaseBackend) -> str:
    name = con.name
    if name not in IBIS_TO_OHDSI_DIALECT:
        print(
            f"Warning: Backend '{name}' not explicitly mapped. Defaulting to '{name}'."
        )
        return name
    return IBIS_TO_OHDSI_DIALECT[name]


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


def _split_sql_statements(sql_script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    escape = False
    for ch in sql_script:
        current.append(ch)
        if ch == "\\" and not escape:
            escape = True
            continue
        if ch == "'" and not in_double and not escape:
            in_single = not in_single
        elif ch == '"' and not in_single and not escape:
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement[:-1].strip())
            current = []
        escape = False
    remainder = "".join(current).strip()
    if remainder:
        statements.append(remainder)
    return statements


def run_python_pipeline(
    con: ibis.BaseBackend,
    json_path: Path,
    cdm_schema: str,
    vocab_schema: str | None,
    *,
    capture_stages: bool = False,
    debug_prefix: str | None = None,
) -> tuple[str, int, dict[str, float], list[dict]]:
    expression = CohortExpression.model_validate_json(json_path.read_text())
    schema = vocab_schema or cdm_schema

    options = CohortBuildOptions(
        cdm_schema=cdm_schema,
        vocabulary_schema=schema,
        capture_sql=capture_stages,
    )

    # Phase 1: Compile Codesets
    compile_start = time.perf_counter()
    resource = compile_codesets(con, expression.concept_sets, options)
    codeset_exec_ms = (time.perf_counter() - compile_start) * 1000

    ctx = BuildContext(con, options, resource)
    stage_details: list[dict[str, object]] = []

    try:
        # Phase 2: Build Events
        build_start = time.perf_counter()
        events = build_primary_events(expression, ctx)
        build_exec_ms = (time.perf_counter() - build_start) * 1000

        if events is None:
            raise RuntimeError("Cohort expression did not produce any primary events.")

        # Phase 3: Compile SQL
        compile_sql_start = time.perf_counter()
        sql = events.compile()
        compile_sql_ms = (time.perf_counter() - compile_sql_start) * 1000

        # Phase 4: Final Execution (Count)
        count_sql = wrap_count_query(sql)
        count_start = time.perf_counter()

        # --- POLARS FIX ---
        # Execute query via Ibis and convert to Polars DataFrame
        pl_df = con.sql(count_sql).to_polars()
        # Extract scalar using Polars item()
        count = int(pl_df.item(0, 0))

        final_exec_ms = (time.perf_counter() - count_start) * 1000

        if capture_stages:
            for idx, (table_name, statement) in enumerate(ctx.captured_sql(), start=1):
                # Generic count via Polars
                c_df = con.sql(f"SELECT COUNT(*) FROM {table_name}").to_polars()
                row_count = int(c_df.item(0, 0))

                stage_details.append(
                    {
                        "index": idx,
                        "table": table_name,
                        "row_count": row_count,
                        "sql": statement,
                    }
                )
    finally:
        if capture_stages and debug_prefix and stage_details:
            for stage in stage_details:
                table_name = stage["table"]
                safe_source = table_name.strip('"')
                target_suffix = re.sub(r"[^A-Za-z0-9_]", "_", safe_source)
                target_name = f"{debug_prefix}_{stage['index']:02d}_{target_suffix}"

                try:
                    ctas = f"CREATE TABLE {quote_ident(target_name)} AS SELECT * FROM {quote_ident(safe_source)}"
                    con.raw_sql(ctas)
                except Exception as e:
                    print(
                        f"Warning: Could not save debug table {target_name}: {e}",
                        file=sys.stderr,
                    )

        ctx.close()

    metrics = {
        "codeset_exec_ms": codeset_exec_ms,
        "build_exec_ms": build_exec_ms,
        "sql_compile_ms": compile_sql_ms,
        "final_exec_ms": final_exec_ms,
        "total_ms": codeset_exec_ms + build_exec_ms + compile_sql_ms + final_exec_ms,
    }
    return sql, count, metrics, stage_details


def generate_circe_sql_via_r(
    json_path: Path,
    target_dialect: str,
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
        if (length(args) != 9) {
          stop("Expected 9 trailing arguments.")
        }
        json_path <- args[[1]]
        cdm_schema <- args[[2]]
        vocab_schema <- args[[3]]
        result_schema <- args[[4]]
        target_schema <- args[[5]]
        target_table <- args[[6]]
        cohort_id <- as.integer(args[[7]])
        temp_schema <- args[[8]]
        target_dialect <- args[[9]]

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
        sql <- SqlRender::translate(sql = sql, targetDialect = target_dialect)
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
        target_dialect,
    ]
    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_ms = (time.perf_counter() - start) * 1000
    if result.returncode != 0:
        raise RuntimeError(
            "Circe SQL generation failed:\n"
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout, elapsed_ms


def execute_circe_sql(
    con: ibis.BaseBackend,
    sql: str,
    result_schema: str,
    target_table: str,
    cohort_id: int,
    temp_schema: str,
) -> tuple[int, dict[str, float]]:
    qualified_table = qualify_identifier(target_table, result_schema)

    if result_schema:
        try:
            con.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(result_schema)}")
        except Exception:
            pass

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            cohort_definition_id BIGINT,
            subject_id BIGINT,
            cohort_start_date DATE,
            cohort_end_date DATE
        )
    """
    try:
        con.raw_sql(create_table_sql)
    except Exception as e:
        print(f"Warning: Failed to ensure target table exists: {e}", file=sys.stderr)

    statements = _split_sql_statements(sql)

    sql_start = time.perf_counter()
    for stmt in statements:
        if not stmt.strip():
            continue
        try:
            con.raw_sql(stmt)
        except Exception as e:
            print(f"Error executing statement:\n{stmt[:200]}...", file=sys.stderr)
            raise e

    sql_exec_ms = (time.perf_counter() - sql_start) * 1000

    count_start = time.perf_counter()
    # --- POLARS FIX ---
    count_query = f"SELECT COUNT(*) FROM {qualified_table} WHERE cohort_definition_id = {cohort_id}"
    pl_df = con.sql(count_query).to_polars()
    row_count = int(pl_df.item(0, 0))

    count_query_ms = (time.perf_counter() - count_start) * 1000

    try:
        con.raw_sql(
            f"DELETE FROM {qualified_table} WHERE cohort_definition_id = {cohort_id}"
        )
    except Exception as e:
        print(f"Warning: Failed to clean up rows: {e}", file=sys.stderr)

    metrics = {"sql_exec_ms": sql_exec_ms, "count_query_ms": count_query_ms}
    return row_count, metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Python/Ibis and Circe-generated cohort row counts."
    )

    parser.add_argument(
        "--config", default="profiles.yml", help="Path to YAML config file."
    )
    parser.add_argument("--profile", help="Name of the profile in YAML to use.")
    parser.add_argument(
        "--backend", help="Override backend (e.g., duckdb, databricks)."
    )

    parser.add_argument(
        "--json",
        default="cohorts/6243-dementia-outcome-v1.json",
        help="Path to cohort JSON.",
    )

    parser.add_argument(
        "--cdm-db", help="Local DuckDB path (Shortcut for simple local runs)."
    )
    parser.add_argument("--cdm-schema", default="main", help="CDM Schema.")
    parser.add_argument("--vocab-schema", help="Vocab Schema.")
    parser.add_argument("--result-schema", help="Result Schema.")
    parser.add_argument("--target-table", default="circe_cohort", help="Target Table.")
    parser.add_argument("--temp-schema", default="scratch", help="Temp Schema.")

    parser.add_argument(
        "--cohort-id", type=int, default=1, help="Cohort Definition ID."
    )

    parser.add_argument("--python-sql-out", help="Save Python SQL to file.")
    parser.add_argument("--circe-sql-out", help="Save Circe SQL to file.")
    parser.add_argument("--python-stage-dir", help="Save intermediate stage SQL.")
    parser.add_argument(
        "--python-debug-prefix", help="Debug prefix for intermediate tables."
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    json_path = Path(args.json)
    if not json_path.exists():
        raise SystemExit(f"Cohort JSON not found: {json_path}")

    con = get_connection(args)
    ohdsi_dialect = get_ohdsi_dialect(con)
    print(f"Detected OHDSI Dialect: {ohdsi_dialect}")

    stage_capture = bool(args.python_stage_dir or args.python_debug_prefix)
    python_sql, python_count, python_metrics, python_stages = run_python_pipeline(
        con=con,
        json_path=json_path,
        cdm_schema=args.cdm_schema,
        vocab_schema=args.vocab_schema,
        capture_stages=stage_capture,
        debug_prefix=args.python_debug_prefix,
    )

    if args.python_sql_out:
        Path(args.python_sql_out).write_text(python_sql)

    result_schema = args.result_schema or args.cdm_schema
    target_schema = result_schema

    circe_sql, circe_gen_ms = generate_circe_sql_via_r(
        json_path=json_path,
        target_dialect=ohdsi_dialect,
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
        con=con,
        sql=circe_sql,
        result_schema=target_schema,
        target_table=args.target_table,
        cohort_id=args.cohort_id,
        temp_schema=args.temp_schema or result_schema,
    )

    python_total_ms = python_metrics["total_ms"]
    circe_total_ms = circe_gen_ms + circe_exec_metrics.get("sql_exec_ms", 0.0)

    print("-" * 60)
    print(f"Python/Ibis row count: {python_count:<10} (exec={python_total_ms:.1f}ms)")
    print(f"Circe row count:       {circe_count:<10} (exec={circe_total_ms:.1f}ms)")
    print("-" * 60)

    if python_count != circe_count:
        print("Row counts do not match.", file=sys.stderr)
        return 1

    print("Row counts match.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Fatal Error: {exc}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        raise SystemExit(1)
