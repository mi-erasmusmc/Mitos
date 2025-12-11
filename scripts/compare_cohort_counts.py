#!/usr/bin/env python1
from __future__ import annotations

import argparse
import os
import re
import sys
import subprocess
import textwrap
import time
from pathlib import Path
from typing import Any, Literal, Annotated

import yaml
import ibis
from ibis.backends import BaseBackend

from pydantic import (
    BaseModel,
    Field,
    FilePath,
    SecretStr,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    model_validator,
)

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events
from mitos.cohort_expression import CohortExpression


IbisConnection = Any
IBIS_TO_OHDSI_DIALECT = {
    "duckdb": "duckdb",
    "databricks": "spark",
    "pyspark": "spark",
    "postgres": "postgresql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "mssql": "sql server",
}


class BaseProfile(BaseModel):
    """Shared settings for all backends (OHDSI specifics)."""

    model_config = ConfigDict(extra="forbid")  # typo protection in YAML!

    cdm_schema: str
    vocab_schema: str | None = None
    result_schema: str | None = None
    temp_schema: str | None = None

    json_path: FilePath = Field(default=Path("cohorts/6243-dementia-outcome-v1.json"))
    cohort_table: str = "circe_cohort"
    cohort_id: int = 1

    capture_stages: bool = False
    debug_prefix: str | None = None
    python_sql_out: Path | None = None
    circe_sql_out: Path | None = None
    python_stage_dir: Path | None = None  # Not DirectoryPath (might not exist yet)

    @model_validator(mode="after")
    def set_defaults(self) -> "BaseProfile":
        if self.vocab_schema is None:
            self.vocab_schema = self.cdm_schema
        if self.python_stage_dir or self.debug_prefix:
            self.capture_stages = True
        return self

    def get_ibis_connection_params(self) -> dict[str, Any]:
        """Subclasses must implement this to return ONLY what ibis.connect needs."""
        raise NotImplementedError


class DuckDBProfile(BaseProfile):
    """Specific Validation for DuckDB Profiles."""

    backend: Literal["duckdb"]

    cdm_schema: str = "main"

    database: str = ":memory:"
    read_only: bool = False

    def get_ibis_connection_params(self) -> dict[str, Any]:
        return {"database": self.database, "read_only": self.read_only}


class DatabricksProfile(BaseProfile):
    """Specific Validation for Databricks Profiles."""

    backend: Literal["databricks"]
    model_config = ConfigDict(populate_by_name=True)
    server_hostname: str = Field(alias="host")
    http_path: str
    access_token: SecretStr
    port: int | None = None
    catalog: str | None = None
    http_headers: dict[str, str] | None = None
    session_configuration: dict[str, str] | None = None
    

    @model_validator(mode="after")
    def validate_token(self) -> "DatabricksProfile":
        token_val = self.access_token.get_secret_value()
        if token_val.startswith("${"):
            raise ValueError(
                f"The access_token appears to be an unresolved variable: '{token_val}'. "
                "Ensure the access_token is set to a valid environment variable."
            )
        return self

    def get_ibis_connection_params(self) -> dict[str, Any]:
        params = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token.get_secret_value(),
        }

        target_catalog = self.catalog
        if not target_catalog and self.result_schema and "." in self.result_schema:
            target_catalog = self.result_schema.split(".")[0]
        if not target_catalog and self.cdm_schema and "." in self.cdm_schema:
            target_catalog = self.cdm_schema.split(".")[0]

        if self.port is not None:
            params["port"] = self.port
        if self.http_headers is not None:
            params["http_headers"] = self.http_headers
        if self.session_configuration is not None:
            params["session_configuration"] = self.session_configuration
        if target_catalog is not None:
            params["catalog"] = target_catalog
        return params


AnyProfile = Annotated[
    DuckDBProfile | DatabricksProfile, Field(discriminator="backend")
]


class ProfilesFile(BaseModel):
    """Validates the entire profiles.yaml file structure."""

    model_config = ConfigDict(extra="ignore")
    default_profile: str | None = None
    profiles: dict[str, AnyProfile]


def load_yaml_with_env(config_path: str) -> dict[str, Any]:
    """
    Loads YAML, substitutes env vars, and transforms flat structure
    into Pydantic-compatible structure.
    """
    if not os.path.exists(config_path):
        return {"profiles": {}}

    with open(config_path, "r") as f:
        try:
            raw_content = f.read()
        except Exception as e:
            raise RuntimeError(f"Error reading config: {e}")

    def sub(match: re.Match[str]) -> str:
        var_name = match.group(1)
        val = os.environ.get(var_name)
        return val if val is not None else match.group(0)

    expanded_content = re.sub(r"\$\{([^}]+)\}", sub, raw_content)

    try:
        data = yaml.safe_load(expanded_content)
    except yaml.YAMLError as e:
        raise RuntimeError(f"Invalid YAML syntax: {e}")

    if not isinstance(data, dict):
        return {"profiles": {}}

    if "profiles" in data:
        return data

    default_profile = data.pop("default_profile", None)

    return {"default_profile": default_profile, "profiles": data}


def resolve_config(args: argparse.Namespace) -> AnyProfile:
    raw_data = load_yaml_with_env(args.config)

    try:
        profiles_obj = ProfilesFile(**raw_data)
    except ValidationError as e:
        print(f"Error in {args.config}:", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(1)
    profile_name = args.profile or profiles_obj.default_profile
    if profile_name:
        if profile_name not in profiles_obj.profiles:
            print(
                f"Profile '{args.profile}' not found in {args.config}", file=sys.stderr
            )
            sys.exit(1)
        print(f"Using profile: {profile_name}")
        active_config = profiles_obj.profiles[profile_name]

        config_dict = active_config.model_dump(mode="python", by_alias=True)
    else:
        config_dict = {"backend": args.backend or "duckdb"}

    meta_keys = {"config", "profile"}
    cli_args = {
        k: v for k, v in vars(args).items() if v is not None and k not in meta_keys
    }

    if cli_args.get("cdm_db"):
        cli_args["database"] = cli_args.pop("cdm_db")

    merged_data = {**config_dict, **cli_args}

    try:
        validator = TypeAdapter(AnyProfile)
        final_config = validator.validate_python(merged_data)
        return final_config
    except ValidationError as e:
        print("Configuration Error (CLI + YAML mismatch):", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(1)


def get_connection(cfg: AnyProfile) -> BaseBackend:
    print(f"Connecting to backend: {cfg.backend}")
    try:
        if not hasattr(ibis, cfg.backend):
            raise ValueError(f"Backend '{cfg.backend}' not recognized.")
        entrypoint = getattr(ibis, cfg.backend)
    except (ImportError, ModuleNotFoundError) as e:
        raise ImportError(
            f"Missing driver for {cfg.backend}. Run `uv add ibis-framework[{cfg.backend}]`"
        ) from e

    try:
        params = cfg.get_ibis_connection_params()
        con = entrypoint.connect(**params)
        return con
    except Exception as e:
        raise RuntimeError(f"Connection failed: {e}") from e


def get_ohdsi_dialect(con: ibis.BaseBackend) -> str:
    return IBIS_TO_OHDSI_DIALECT.get(con.name, con.name)


def quote_ident(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def qualify_identifier(name: str, schema: str | None) -> str:
    if not schema:
        return quote_ident(name)
    parts = schema.split(".")
    quoted_schema = ".".join(quote_ident(p) for p in parts)
    return f"{quoted_schema}.{quote_ident(name)}"


def wrap_count_query(sql: str) -> str:
    trimmed = sql.strip().rstrip(";")
    return f"SELECT COUNT(*) AS row_count FROM ({trimmed}) as cohort_rows"


def _split_sql_statements(sql_script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single = in_double = escape = False
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
            if s := "".join(current).strip():
                statements.append(s[:-1].strip())
            current = []
        escape = False
    if s := "".join(current).strip():
        statements.append(s)
    return statements


def run_python_pipeline(
    con: IbisConnection,
    cfg: AnyProfile,
) -> tuple[str, int, dict[str, float], list[dict]]:
    expression = CohortExpression.model_validate_json(cfg.json_path.read_text())

    options = CohortBuildOptions(
        cdm_schema=cfg.cdm_schema,
        vocabulary_schema=cfg.vocab_schema,
        capture_sql=cfg.capture_stages,
    )

    compile_start = time.perf_counter()
    resource = compile_codesets(con, expression.concept_sets, options)
    codeset_exec_ms = (time.perf_counter() - compile_start) * 1000

    ctx = BuildContext(con, options, resource)
    stage_details: list[dict[str, object]] = []

    try:
        build_start = time.perf_counter()
        events = build_primary_events(expression, ctx)
        build_exec_ms = (time.perf_counter() - build_start) * 1000

        if events is None:
            raise RuntimeError("Cohort expression produced no primary events.")

        compile_sql_start = time.perf_counter()
        sql = events.compile()
        compile_sql_ms = (time.perf_counter() - compile_sql_start) * 1000

        count_sql = wrap_count_query(str(sql))
        count_start = time.perf_counter()

        pl_df = con.sql(count_sql).to_polars()
        count = int(pl_df.item(0, 0))

        final_exec_ms = (time.perf_counter() - count_start) * 1000

        if cfg.capture_stages:
            for idx, (table_name, statement) in enumerate(ctx.captured_sql(), start=1):
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
        if cfg.capture_stages and cfg.debug_prefix and stage_details:
            for stage in stage_details:
                safe_src = stage["table"].strip('"')
                safe_suffix = re.sub(r"[^A-Za-z0-9_]", "_", safe_src)
                target = f"{cfg.debug_prefix}_{stage['index']:02d}_{safe_suffix}"
                try:
                    con.raw_sql(
                        f"CREATE TABLE {quote_ident(target)} AS SELECT * FROM {quote_ident(safe_src)}"
                    )
                except Exception as e:
                    print(
                        f"Warning: Failed to save debug table {target}: {e}",
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
    return str(sql), count, metrics, stage_details


def generate_circe_sql_via_r(
    cfg: AnyProfile,
    target_dialect: str,
) -> tuple[str, float]:
    temp_arg = cfg.temp_schema or ""
    target_schema_arg = cfg.result_schema or cfg.cdm_schema
    result_schema_arg = cfg.result_schema or cfg.cdm_schema

    r_script = textwrap.dedent(
        """
        suppressPackageStartupMessages({library(CirceR); library(SqlRender)})
        args <- commandArgs(trailingOnly = TRUE)
        json_path <- args[[1]]; cdm_schema <- args[[2]]; vocab_schema <- args[[3]]
        result_schema <- args[[4]]; target_schema <- args[[5]]; target_table <- args[[6]]
        cohort_id <- as.integer(args[[7]]); temp_schema <- args[[8]]; target_dialect <- args[[9]]

        json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\\n")
        expression <- CirceR::cohortExpressionFromJson(json_str)
        options <- CirceR::createGenerateOptions()
        options$generateStats <- FALSE; options$useTempTables <- FALSE;
        options$tempEmulationSchema <- if(temp_schema=="") NULL else temp_schema
        
        sql <- CirceR::buildCohortQuery(expression, options)
        sql <- SqlRender::render(sql, cdm_database_schema=cdm_schema, 
                                 vocabulary_database_schema=vocab_schema,
                                 results_database_schema=result_schema,
                                 target_database_schema=target_schema,
                                 target_cohort_table=target_table,
                                 target_cohort_id=cohort_id,
                                 tempEmulationSchema=if(temp_schema=="") NULL else temp_schema)
        cat(SqlRender::translate(sql=sql, targetDialect=target_dialect))
        """
    ).strip()

    cmd = [
        "Rscript",
        "-e",
        r_script,
        str(cfg.json_path),
        cfg.cdm_schema,
        cfg.vocab_schema,
        result_schema_arg,
        target_schema_arg,
        cfg.cohort_table,
        str(cfg.cohort_id),
        temp_arg,
        target_dialect,
    ]

    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - start) * 1000
    if result.returncode != 0:
        raise RuntimeError(f"Circe R Error:\n{result.stderr or result.stdout}")
    return result.stdout, elapsed


def execute_circe_sql(
    con: IbisConnection,
    cfg: AnyProfile,
    sql: str,
) -> tuple[int, dict[str, float]]:
    target_schema = cfg.result_schema or cfg.cdm_schema
    qualified_table = qualify_identifier(cfg.cohort_table, target_schema)

    if cfg.result_schema:
        try:
            con.raw_sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(cfg.result_schema)}")
        except Exception:
            pass

    try:
        con.raw_sql(f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                cohort_definition_id BIGINT, subject_id BIGINT, 
                cohort_start_date DATE, cohort_end_date DATE
            )
        """)
    except Exception as e:
        print(f"Warning: Target table ensure failed: {e}", file=sys.stderr)

    statements = _split_sql_statements(sql)
    sql_start = time.perf_counter()
    for stmt in statements:
        if stmt.strip():
            try:
                con.raw_sql(stmt)
            except Exception as e:
                print(f"SQL Fail:\n{stmt[:100]}...", file=sys.stderr)
                raise e
    sql_ms = (time.perf_counter() - sql_start) * 1000

    count_start = time.perf_counter()
    count_query = f"SELECT COUNT(*) FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}"
    pl_df = con.sql(count_query).to_polars()
    row_count = int(pl_df.item(0, 0))
    count_ms = (time.perf_counter() - count_start) * 1000

    try:
        con.raw_sql(
            f"DELETE FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}"
        )
    except Exception:
        pass

    return row_count, {"sql_exec_ms": sql_ms, "count_query_ms": count_ms}


# -----------------------------------------------------------------------------
# 5. MAIN
# -----------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="profiles.yaml")
    parser.add_argument("--profile")
    parser.add_argument("--backend")  # Can strictly change the Pydantic model used!

    # Pydantic will validate these paths exist when we merge them
    parser.add_argument("--json")
    parser.add_argument("--cdm-db")

    parser.add_argument("--cdm-schema")
    parser.add_argument("--vocab-schema")
    parser.add_argument("--result-schema")
    parser.add_argument("--target-table")
    parser.add_argument("--temp-schema")
    parser.add_argument("--cohort-id", type=int)

    parser.add_argument("--python-sql-out")
    parser.add_argument("--circe-sql-out")
    parser.add_argument("--python-stage-dir")
    parser.add_argument("--python-debug-prefix")
    return parser.parse_args()


def main():
    try:
        args = parse_args()

        # 1. The Pydantic Power Move
        # Validate YAML file -> Pick Profile -> Merge CLI -> Validate Result
        cfg = resolve_config(args)

        # 2. Connect
        # cfg is now strictly typed (e.g. DatabricksProfile).
        # cfg.access_token exists. cfg.database might NOT exist if it's Databricks.
        con = get_connection(cfg)

        dialect = get_ohdsi_dialect(con)
        print(f"Dialect: {dialect} | Backend: {cfg.backend}")
        print(f"CDM: {cfg.cdm_schema}")

        # 3. PYTHON PIPELINE
        py_sql, py_count, py_metrics, py_stages = run_python_pipeline(con, cfg)
        if cfg.python_sql_out:
            cfg.python_sql_out.write_text(py_sql)
        if cfg.python_stage_dir and py_stages:
            cfg.python_stage_dir.mkdir(parents=True, exist_ok=True)
            # Stage saving logic omitted for brevity

        # 4. CIRCE PIPELINE
        circe_sql, circe_gen_ms = generate_circe_sql_via_r(cfg, dialect)
        if cfg.circe_sql_out:
            cfg.circe_sql_out.write_text(circe_sql)

        circe_count, circe_metrics = execute_circe_sql(con, cfg, circe_sql)

        # 5. REPORT
        print("-" * 60)
        print(f"Python: {py_count:<10} (Total {py_metrics['total_ms']:.1f}ms)")
        print(
            f"Circe:  {circe_count:<10} (Total {circe_gen_ms + circe_metrics['sql_exec_ms']:.1f}ms)"
        )
        print("-" * 60)

        return 0 if py_count == circe_count else 1

    except Exception as e:
        print(f"Fatal Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    main()
