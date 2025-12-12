#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import re
import sys
import subprocess
import textwrap
import time
import shutil
import tempfile
import contextlib
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
    skip_circe: bool = False
    rscript_path: str | None = None
    circe_debug: bool = False
    cleanup_circe: bool = True

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
    schema: str | None = None
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
        def _split_catalog_schema(value: str) -> tuple[str | None, str | None]:
            value = value.strip()
            if not value:
                return None, None
            if "." not in value:
                return None, value
            catalog, schema = value.split(".", 1)
            return catalog or None, schema or None

        # Databricks connections need a usable "current" catalog/schema.
        # Prefer the writable destination (result_schema) when present, otherwise
        # fall back to cdm_schema.
        default_catalog, default_schema = _split_catalog_schema(
            (self.result_schema or self.cdm_schema or "").strip()
        )

        params = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token.get_secret_value(),
        }

        if self.port is not None:
            params["port"] = self.port
        if self.http_headers is not None:
            params["http_headers"] = self.http_headers
        if self.session_configuration is not None:
            params["session_configuration"] = self.session_configuration
        # Allow explicit override, otherwise use derived defaults.
        catalog = self.catalog or default_catalog
        schema = self.schema or default_schema
        if catalog is not None:
            params["catalog"] = catalog
        if schema is not None:
            params["schema"] = schema
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

    if cli_args.pop("no_cleanup_circe", False):
        cli_args["cleanup_circe"] = False

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

def quote_ident_for_backend(value: str, backend: str) -> str:
    if backend == "databricks":
        escaped = value.replace("`", "``")
        return f"`{escaped}`"
    return quote_ident(value)


def qualify_identifier(name: str, schema: str | None) -> str:
    if not schema:
        return quote_ident(name)
    parts = schema.split(".")
    quoted_schema = ".".join(quote_ident(p) for p in parts)
    return f"{quoted_schema}.{quote_ident(name)}"

def qualify_identifier_for_backend(name: str, schema: str | None, backend: str) -> str:
    if not schema:
        return quote_ident_for_backend(name, backend)
    parts = schema.split(".")
    quoted_schema = ".".join(quote_ident_for_backend(p, backend) for p in parts)
    return f"{quoted_schema}.{quote_ident_for_backend(name, backend)}"


def wrap_count_query(sql: str) -> str:
    trimmed = sql.strip().rstrip(";")
    return f"SELECT COUNT(*) AS row_count FROM ({trimmed}) as cohort_rows"

def _exec_raw(con: IbisConnection, sql: str) -> None:
    """
    Execute a statement via `raw_sql` and eagerly consume results.

    Databricks SQL can behave asynchronously; consuming results and closing the cursor
    ensures completion and avoids resource leaks.
    """
    cur = con.raw_sql(sql)
    try:
        try:
            cur.fetchall()
        except Exception:
            pass
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _fetch_scalar(con: IbisConnection, sql: str):
    cur = con.raw_sql(sql)
    try:
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _split_sql_statements(sql_script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single = in_double = in_backtick = False
    in_line_comment = in_block_comment = False
    escape = False
    i = 0
    n = len(sql_script)
    while i < n:
        ch = sql_script[i]
        nxt = sql_script[i + 1] if i + 1 < n else ""
        current.append(ch)
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                current.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if not (in_single or in_double or in_backtick):
            if ch == "-" and nxt == "-":
                current.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                current.append(nxt)
                in_block_comment = True
                i += 2
                continue

        if ch == "\\" and not escape:
            escape = True
            i += 1
            continue

        if ch == "'" and not (in_double or in_backtick) and not escape:
            in_single = not in_single
        elif ch == '"' and not (in_single or in_backtick) and not escape:
            in_double = not in_double
        elif ch == "`" and not (in_single or in_double) and not escape:
            in_backtick = not in_backtick
        elif ch == ";" and not (in_single or in_double or in_backtick):
            if s := "".join(current).strip():
                statements.append(s[:-1].strip())
            current = []

        escape = False
        i += 1
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
        temp_emulation_schema=cfg.temp_schema,
        capture_sql=cfg.capture_stages,
        backend=cfg.backend,
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
        sql = con.compile(events)
        compile_sql_ms = (time.perf_counter() - compile_sql_start) * 1000

        count_start = time.perf_counter()
        count = int(events.count().execute())

        final_exec_ms = (time.perf_counter() - count_start) * 1000

        if cfg.capture_stages:
            for idx, (table_name, statement) in enumerate(ctx.captured_sql(), start=1):
                stage_db = cfg.temp_schema
                stage_tbl = (
                    con.table(table_name, database=stage_db)
                    if stage_db
                    else con.table(table_name)
                )
                row_count = int(stage_tbl.count().execute())
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
                    src_table = (
                        con.table(safe_src, database=cfg.temp_schema)
                        if cfg.temp_schema
                        else con.table(safe_src)
                    )
                    con.create_table(target, obj=src_table, overwrite=True)
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
    rscript_exe = cfg.rscript_path or shutil.which("Rscript")
    if not rscript_exe:
        raise RuntimeError(
            "Rscript executable not found. Install R and ensure `Rscript` is on PATH, "
            "or set `rscript_path` in your profile / pass `--rscript-path`."
        )
    temp_arg = cfg.temp_schema or ""
    target_schema_arg = cfg.result_schema or cfg.cdm_schema
    result_schema_arg = cfg.result_schema or cfg.cdm_schema

    # Use a temp script file + output file for robust Windows execution.
    tmp_dir = Path(tempfile.mkdtemp(prefix="mitos_circe_"))
    out_path = tmp_dir / "circe.sql"
    script_path = tmp_dir / "circe.R"

    r_script = textwrap.dedent(
        r"""
        suppressPackageStartupMessages({library(CirceR); library(SqlRender)})
        args <- commandArgs(trailingOnly = TRUE)
        json_path <- args[[1]]; cdm_schema <- args[[2]]; vocab_schema <- args[[3]]
        result_schema <- args[[4]]; target_schema <- args[[5]]; target_table <- args[[6]]
        cohort_id <- as.integer(args[[7]]); temp_schema <- args[[8]]; target_dialect <- args[[9]]
        out_path <- args[[10]]

        out_path <- normalizePath(out_path, winslash = "/", mustWork = FALSE)
        cat("MITOS_CIRCE_BEGIN\n", file=stderr())
        cat(paste0("MITOS_CIRCE_JSON_PATH=", json_path, "\n"), file=stderr())
        cat(paste0("MITOS_CIRCE_OUT_PATH=", out_path, "\n"), file=stderr())

        json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
        if (length(json_str) == 0 || nchar(json_str) == 0) {
          cat("MITOS_CIRCE_EMPTY_JSON\n", file=stderr())
          quit(status = 3)
        }

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
        translated <- SqlRender::translate(sql=sql, targetDialect=target_dialect)
        if (length(translated) == 0 || nchar(translated) == 0) {
          cat("MITOS_CIRCE_EMPTY_SQL\n", file=stderr())
          quit(status = 2)
        }
        writeLines(translated, out_path, useBytes = TRUE)
        cat("MITOS_CIRCE_WROTE_SQL\n", file=stderr())
        """
    ).strip()
    script_path.write_text(r_script, encoding="utf-8")

    cmd = [
        rscript_exe,
        "--vanilla",
        str(script_path),
        str(cfg.json_path),
        cfg.cdm_schema,
        cfg.vocab_schema,
        result_schema_arg,
        target_schema_arg,
        cfg.cohort_table,
        str(cfg.cohort_id),
        temp_arg,
        target_dialect,
        str(out_path),
    ]

    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - start) * 1000
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    if result.returncode != 0:
        raise RuntimeError(
            "Circe R Error.\n"
            f"returncode={result.returncode}\n"
            f"stderr:\n{stderr or '<empty>'}\n"
            f"stdout:\n{stdout or '<empty>'}\n"
        )

    try:
        sql_text = out_path.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        sql_text = ""
    finally:
        with contextlib.suppress(Exception):
            out_path.unlink()
        with contextlib.suppress(Exception):
            script_path.unlink()
        with contextlib.suppress(Exception):
            tmp_dir.rmdir()
        with contextlib.suppress(Exception):
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if not sql_text:
        raise RuntimeError(
            "Circe SQL generation returned empty output.\n"
            "R returned success but produced no SQL file content.\n"
            f"stderr:\n{stderr or '<empty>'}\n"
            f"stdout:\n{stdout or '<empty>'}\n"
        )

    return sql_text, elapsed


def execute_circe_sql(
    con: IbisConnection,
    cfg: AnyProfile,
    sql: str,
) -> tuple[int, dict[str, float]]:
    target_schema = cfg.result_schema or cfg.cdm_schema
    qualified_table = qualify_identifier_for_backend(
        cfg.cohort_table, target_schema, cfg.backend
    )

    if cfg.result_schema:
        try:
            _exec_raw(
                con,
                f"CREATE SCHEMA IF NOT EXISTS {'.'.join(quote_ident_for_backend(p, cfg.backend) for p in cfg.result_schema.split('.'))}"
            )
        except Exception as e:
            if cfg.backend == "databricks":
                raise RuntimeError(
                    f"Failed to create result schema {cfg.result_schema!r}. "
                    "Set `result_schema` to a writable catalog.schema or pre-create it with the right permissions."
                ) from e

    try:
        _exec_raw(con, f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                cohort_definition_id BIGINT, subject_id BIGINT, 
                cohort_start_date DATE, cohort_end_date DATE
            )
        """)
    except Exception as e:
        if cfg.backend == "databricks":
            raise RuntimeError(
                f"Failed to ensure Circe target table {qualified_table}. "
                "Set `result_schema` to a writable schema or pre-create the table."
            ) from e
        print(f"Warning: Target table ensure failed: {e}", file=sys.stderr)

    statements = _split_sql_statements(sql)
    sql_start = time.perf_counter()
    for idx, stmt in enumerate(statements, start=1):
        if not stmt.strip():
            continue
        if cfg.circe_debug:
            preview = " ".join(stmt.strip().split())[:160]
            print(f"[Circe SQL {idx}/{len(statements)}] {preview}")
        try:
            _exec_raw(con, stmt)
        except Exception as e:
            print(f"SQL Fail (stmt {idx}/{len(statements)}):\n{stmt[:400]}...", file=sys.stderr)
            raise e
    sql_ms = (time.perf_counter() - sql_start) * 1000

    count_start = time.perf_counter()
    count_query = f"SELECT COUNT(*) FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}"
    target_db = cfg.result_schema or cfg.cdm_schema
    cohort_tbl = (
        con.table(cfg.cohort_table, database=target_db)
        if target_db
        else con.table(cfg.cohort_table)
    )
    row_count = int(
        cohort_tbl.filter(cohort_tbl.cohort_definition_id == cfg.cohort_id)
        .count()
        .execute()
    )
    count_ms = (time.perf_counter() - count_start) * 1000

    if cfg.circe_debug or row_count == 0:
        try:
            total = _fetch_scalar(con, f"SELECT COUNT(*) FROM {qualified_table}")
            by_id = _fetch_scalar(
                con,
                f"SELECT COUNT(*) FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}",
            )
            print(
                f"[Circe Debug] target={qualified_table} total_rows={total} rows_for_id={by_id}"
            )
        except Exception as e:
            print(f"[Circe Debug] Could not query target table: {e}", file=sys.stderr)

    if cfg.cleanup_circe:
        try:
            _exec_raw(
                con,
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
    parser.add_argument("--skip-circe", action="store_true")
    parser.add_argument("--rscript-path")
    parser.add_argument("--circe-debug", action="store_true")
    parser.add_argument("--no-cleanup-circe", action="store_true")
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
        if cfg.skip_circe:
            print("-" * 60)
            print(f"Python: {py_count:<10} (Total {py_metrics['total_ms']:.1f}ms)")
            print("Circe:  skipped")
            print("-" * 60)
            return 0

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
