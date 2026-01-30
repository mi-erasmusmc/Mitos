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
import uuid
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
from mitos.sql_split import split_sql_statements


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
    cohort_table: str = Field(default="circe_cohort", alias="target_table")
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
    # Prefer lazy (non-materialized) expressions by default for portability/perf.
    python_materialize_stages: bool = False
    python_materialize_codesets: bool = True
    trace_subjects_from: Literal["both", "missing-in-circe", "missing-in-python"] = (
        "both"
    )

    @model_validator(mode="after")
    def set_defaults(self) -> "BaseProfile":
        if self.vocab_schema is None:
            self.vocab_schema = self.cdm_schema
        if self.python_stage_dir or self.debug_prefix:
            self.capture_stages = True
            # If the user wants stage output, we must materialize stages.
            self.python_materialize_stages = True
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
    connect_schema: str | None = Field(default=None, alias="schema")
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
        schema = self.connect_schema or default_schema
        if catalog is not None:
            params["catalog"] = catalog
        if schema is not None:
            params["schema"] = schema
        return params


class PostgresProfile(BaseProfile):
    """Specific Validation for Postgres Profiles."""

    backend: Literal["postgres"]

    host: str
    port: int = 5432
    user: str
    password: SecretStr | None = None
    database: str

    sslmode: str | None = None
    connect_timeout: int | None = None

    @model_validator(mode="after")
    def validate_password(self) -> "PostgresProfile":
        if self.password is not None:
            val = self.password.get_secret_value()
            if val.startswith("${"):
                raise ValueError(
                    f"The password appears to be an unresolved variable: '{val}'. "
                    "Ensure it is set via environment variable expansion in your profiles.yaml."
                )
        return self

    def get_ibis_connection_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {
            "host": self.host,
            "port": int(self.port),
            "user": self.user,
            "database": self.database,
        }
        if self.password is not None:
            params["password"] = self.password.get_secret_value()
        if self.sslmode is not None:
            params["sslmode"] = self.sslmode
        if self.connect_timeout is not None:
            params["connect_timeout"] = int(self.connect_timeout)
        return params


AnyProfile = Annotated[
    DuckDBProfile | DatabricksProfile | PostgresProfile, Field(discriminator="backend")
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

    meta_keys = {
        "config",
        "profile",
        "explain_dir",
        "diff",
        "diff_limit",
        "trace_stages",
        "trace_subject_limit",
        "diff_report",
    }
    cli_args = {
        k: v for k, v in vars(args).items() if v is not None and k not in meta_keys
    }

    # Backwards/CLI-friendly flag names that map to profile fields.
    if cli_args.get("python_debug_prefix") is not None:
        cli_args["debug_prefix"] = cli_args.pop("python_debug_prefix")

    if cli_args.get("json"):
        cli_args["json_path"] = Path(cli_args.pop("json"))

    if cli_args.get("cdm_db"):
        cli_args["database"] = cli_args.pop("cdm_db")

    if cli_args.pop("no_cleanup_circe", False):
        cli_args["cleanup_circe"] = False

    if cli_args.pop("no_python_stages", False):
        cli_args["python_materialize_stages"] = False

    if cli_args.pop("inline_python_codesets", False):
        cli_args["python_materialize_codesets"] = False

    if cli_args.pop("python_stages", False):
        cli_args["python_materialize_stages"] = True

    if cli_args.pop("python_materialize_codesets", False):
        cli_args["python_materialize_codesets"] = True

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


def _sql_count(con: IbisConnection, qualified: str) -> int:
    return int(_fetch_scalar(con, f"SELECT COUNT(*) FROM {qualified}"))


def _sql_count_for_ids(
    con: IbisConnection,
    qualified: str,
    *,
    id_column: str,
    ids: list[int],
) -> int:
    if not ids:
        return 0
    id_list = ", ".join(str(int(v)) for v in ids)
    return int(
        _fetch_scalar(
            con, f"SELECT COUNT(*) FROM {qualified} WHERE {id_column} IN ({id_list})"
        )
    )


def _sql_person_summary(
    con: IbisConnection,
    qualified: str,
    *,
    id_column: str,
    ids: list[int],
    extra_columns_sql: str = "",
) -> list[tuple]:
    if not ids:
        return []
    id_list = ", ".join(str(int(v)) for v in ids)
    sql = (
        f"SELECT {id_column} AS person_id, COUNT(*) AS n {extra_columns_sql} "
        f"FROM {qualified} "
        f"WHERE {id_column} IN ({id_list}) "
        f"GROUP BY {id_column} "
        f"ORDER BY {id_column}"
    )
    cur = con.raw_sql(sql)
    try:
        return cur.fetchall()
    finally:
        try:
            underlying = getattr(con, "con", None)
            if underlying is None or cur is not underlying:
                cur.close()
        except Exception:
            pass


def _sql_fetch_rows(con: IbisConnection, sql: str) -> tuple[list[str], list[tuple]]:
    cur = con.raw_sql(sql)
    try:
        cols = [d[0] for d in (cur.description or [])]
        rows = cur.fetchall()
        return cols, rows
    finally:
        try:
            underlying = getattr(con, "con", None)
            if underlying is None or cur is not underlying:
                cur.close()
        except Exception:
            pass


def _format_rows_as_tsv(columns: list[str], rows: list[tuple]) -> str:
    if not rows:
        return "<empty>"
    header = "\t".join(columns)
    body = "\n".join("\t".join("" if v is None else str(v) for v in r) for r in rows)
    return f"{header}\n{body}"


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
        # Ibis backends differ:
        # - Databricks raw_sql returns a cursor that should be closed.
        # - DuckDB raw_sql returns the underlying DuckDB connection object; calling
        #   close() would close the entire connection and break subsequent statements.
        try:
            underlying = getattr(con, "con", None)
            if underlying is None or cur is not underlying:
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
            underlying = getattr(con, "con", None)
            if underlying is None or cur is not underlying:
                cur.close()
        except Exception:
            pass


def explain_formatted(con: IbisConnection, sql: str) -> str:
    trimmed = sql.strip().rstrip(";")
    cur = con.raw_sql(f"EXPLAIN FORMATTED {trimmed}")
    try:
        rows = cur.fetchall()
    finally:
        try:
            underlying = getattr(con, "con", None)
            if underlying is None or cur is not underlying:
                cur.close()
        except Exception:
            pass
    parts: list[str] = []
    for row in rows or []:
        if isinstance(row, tuple):
            parts.append("\t".join("" if v is None else str(v) for v in row))
        else:
            parts.append(str(row))
    return "\n".join(parts).strip()


def _qualify_databricks_schema_parts(schema: str) -> tuple[str | None, str]:
    parts = schema.split(".", 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]


def _set_databricks_current_schema(con: IbisConnection, schema: str) -> None:
    catalog, sch = _qualify_databricks_schema_parts(schema)
    if catalog:
        _exec_raw(con, f"USE CATALOG {quote_ident_for_backend(catalog, 'databricks')}")
    _exec_raw(con, f"USE SCHEMA {quote_ident_for_backend(sch, 'databricks')}")


def _rewrite_circe_temp_table_qualification(
    sql_script: str,
    *,
    temp_schema: str,
    backend: str,
) -> str:
    """
    Circe-generated temp table names are typically unqualified. On Databricks the session
    schema may not be stable across statements/cursors; qualify temp tables to a known schema.
    """
    statements = _split_sql_statements(sql_script)
    temp_tables: set[str] = set()

    for stmt in statements:
        m = re.match(
            r"(?is)\s*create\s+table\s+(?:if\s+not\s+exists\s+)?([A-Za-z0-9_]+)\b",
            stmt,
        )
        if m:
            name = m.group(1)
            if "." not in name:
                temp_tables.add(name)

    if not temp_tables:
        return sql_script

    out: list[str] = []
    for stmt in statements:
        rewritten = stmt
        for name in sorted(temp_tables, key=len, reverse=True):
            qualified = qualify_identifier_for_backend(name, temp_schema, backend)
            # Replace only unqualified identifier occurrences (avoid already-qualified).
            rewritten = re.sub(
                rf"(?<![\w.]){re.escape(name)}(?![\w])",
                qualified,
                rewritten,
            )
        out.append(rewritten)
    return ";\n".join(out)


def _extract_circe_select_for_explain(sql_script: str) -> str | None:
    """
    Heuristic: pick the heaviest CTAS step and extract its SELECT for EXPLAIN.
    Prefer qualified_events, then final_cohort, then cohort_rows.
    """
    statements = _split_sql_statements(sql_script)
    targets = ("qualified_events", "final_cohort", "cohort_rows")
    for suffix in targets:
        for stmt in statements:
            normalized = " ".join(stmt.strip().split())
            if not normalized.lower().startswith("create table "):
                continue
            if suffix.lower() not in normalized.lower():
                continue
            # Look for "... AS <select>"
            m = re.search(r"\bAS\b", stmt, flags=re.IGNORECASE)
            if not m:
                continue
            select_part = stmt[m.end() :].strip()
            if select_part.lower().startswith("select"):
                return select_part
    return None


def _split_sql_statements(sql_script: str) -> list[str]:
    return split_sql_statements(sql_script)


def run_python_pipeline(
    con: IbisConnection,
    cfg: AnyProfile,
    *,
    keep_context_open: bool = False,
    diff: bool = False,
) -> tuple[
    str, int, dict[str, float], list[dict], BuildContext | None, str | None, str | None
]:
    expression = CohortExpression.model_validate_json(cfg.json_path.read_text())

    options = CohortBuildOptions(
        cdm_schema=cfg.cdm_schema,
        vocabulary_schema=cfg.vocab_schema,
        temp_emulation_schema=cfg.temp_schema,
        capture_sql=cfg.capture_stages,
        backend=cfg.backend,
        materialize_stages=cfg.python_materialize_stages,
        materialize_codesets=cfg.python_materialize_codesets,
    )

    compile_start = time.perf_counter()
    resource = compile_codesets(con, expression.concept_sets, options)
    codeset_exec_ms = (time.perf_counter() - compile_start) * 1000

    ctx = BuildContext(con, options, resource)
    stage_details: list[dict[str, object]] = []

    python_diff_table: str | None = None
    python_diff_db: str | None = None

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

        if diff:
            python_diff_db = cfg.temp_schema or cfg.result_schema
            if python_diff_db is None and cfg.backend == "duckdb":
                python_diff_db = cfg.cdm_schema
            python_diff_table = f"_mitos_python_cohort_rows_{uuid.uuid4().hex}"
            try:
                cohort_rows = events.select(
                    cohort_definition_id=ibis.literal(int(cfg.cohort_id)).cast("int64"),
                    subject_id=events.person_id.cast("int64"),
                    cohort_start_date=events.start_date.cast("date"),
                    cohort_end_date=events.end_date.cast("date"),
                )
                con.create_table(
                    python_diff_table,
                    obj=cohort_rows,
                    database=python_diff_db,
                    temp=False,
                    overwrite=True,
                )
            except Exception as e:
                python_diff_table = None
                python_diff_db = None
                print(
                    f"Warning: failed to materialize python cohort rows for diff: {e}",
                    file=sys.stderr,
                )

        if cfg.capture_stages:
            want_row_counts = bool(cfg.python_stage_dir)
            for idx, (table_name, statement) in enumerate(ctx.captured_sql(), start=1):
                stage: dict[str, object] = {
                    "index": idx,
                    "table": table_name,
                    "sql": statement,
                }
                if want_row_counts:
                    stage_db = cfg.temp_schema
                    stage_tbl = (
                        con.table(table_name, database=stage_db)
                        if stage_db
                        else con.table(table_name)
                    )
                    stage["row_count"] = int(stage_tbl.count().execute())
                stage_details.append(stage)
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
        if not keep_context_open:
            ctx.close()

    metrics = {
        "codeset_exec_ms": codeset_exec_ms,
        "build_exec_ms": build_exec_ms,
        "sql_compile_ms": compile_sql_ms,
        "final_exec_ms": final_exec_ms,
        "total_ms": codeset_exec_ms + build_exec_ms + compile_sql_ms + final_exec_ms,
    }
    return (
        str(sql),
        count,
        metrics,
        stage_details,
        (ctx if keep_context_open else None),
        python_diff_table,
        python_diff_db,
    )


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
    *,
    explain_dir: Path | None = None,
    explain_prefix: str = "",
    preserve_temp_tables: bool = False,
) -> tuple[int, dict[str, Any]]:
    target_schema = cfg.result_schema or cfg.cdm_schema
    qualified_table = qualify_identifier_for_backend(
        cfg.cohort_table, target_schema, cfg.backend
    )

    if cfg.result_schema:
        try:
            _exec_raw(
                con,
                f"CREATE SCHEMA IF NOT EXISTS {'.'.join(quote_ident_for_backend(p, cfg.backend) for p in cfg.result_schema.split('.'))}",
            )
        except Exception as e:
            if cfg.backend == "databricks":
                raise RuntimeError(
                    f"Failed to create result schema {cfg.result_schema!r}. "
                    "Set `result_schema` to a writable catalog.schema or pre-create it with the right permissions."
                ) from e

    try:
        _exec_raw(
            con,
            f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
                cohort_definition_id BIGINT, subject_id BIGINT, 
                cohort_start_date DATE, cohort_end_date DATE
            )
        """,
        )
    except Exception as e:
        if cfg.backend == "databricks":
            raise RuntimeError(
                f"Failed to ensure Circe target table {qualified_table}. "
                "Set `result_schema` to a writable schema or pre-create the table."
            ) from e
        print(f"Warning: Target table ensure failed: {e}", file=sys.stderr)

    if cfg.backend == "databricks":
        # Circe temp tables are often unqualified; make resolution robust.
        if cfg.temp_schema:
            _set_databricks_current_schema(con, cfg.temp_schema)
            sql = _rewrite_circe_temp_table_qualification(
                sql,
                temp_schema=cfg.temp_schema,
                backend=cfg.backend,
            )
        elif cfg.result_schema:
            _set_databricks_current_schema(con, cfg.result_schema)

    circe_stage_tables: dict[str, str] = {}

    def _maybe_record_stage_table(stmt: str) -> None:
        # Handle statements that may include leading comments before the CREATE.
        m = re.search(
            r"\bcreate\s+(?:temp\s+)?table\s+(?:if\s+not\s+exists\s+)?([^\s(]+)",
            stmt,
            flags=re.IGNORECASE,
        )
        if not m:
            return
        name = m.group(1).strip().rstrip(";")
        lowered = name.lower()
        for label in (
            "codesets",
            "qualified_events",
            "inclusion_events",
            "included_events",
            "strategy_ends",
            "cohort_rows",
            "final_cohort",
        ):
            if label in lowered:
                circe_stage_tables[label] = name
                return

    def _is_preserved_table_name(token: str) -> bool:
        if not token:
            return False
        candidate = token.strip().rstrip(";")
        cand_lower = candidate.lower()
        for name in circe_stage_tables.values():
            if cand_lower == name.strip().rstrip(";").lower():
                return True
        return False

    def _should_skip_stmt(stmt: str) -> bool:
        if not preserve_temp_tables:
            return False
        normalized = " ".join(stmt.strip().split()).lower()
        if normalized.startswith("drop table "):
            # DROP TABLE [IF EXISTS] <name>
            m = re.match(
                r"^drop\s+table\s+(?:if\s+exists\s+)?([^\s;]+)",
                stmt.strip(),
                flags=re.IGNORECASE,
            )
            return _is_preserved_table_name(m.group(1)) if m else False
        if normalized.startswith("truncate table "):
            m = re.match(
                r"^truncate\s+table\s+([^\s;]+)",
                stmt.strip(),
                flags=re.IGNORECASE,
            )
            return _is_preserved_table_name(m.group(1)) if m else False
        if normalized.startswith("delete from "):
            # Keep deletes against the target cohort table (Circe uses this to clear by cohort_definition_id),
            # and skip deletes for stage tables we want preserved.
            m = re.match(
                r"^delete\s+from\s+([^\s;]+)",
                stmt.strip(),
                flags=re.IGNORECASE,
            )
            if not m:
                return False
            token = m.group(1)
            if cfg.cohort_table.lower() in token.lower():
                return False
            return _is_preserved_table_name(token)
        return False

    statements = _split_sql_statements(sql)
    sql_start = time.perf_counter()
    for idx, stmt in enumerate(statements, start=1):
        if not stmt.strip():
            continue
        _maybe_record_stage_table(stmt)
        if _should_skip_stmt(stmt):
            if cfg.circe_debug:
                preview = " ".join(stmt.strip().split())[:160]
                print(f"[Circe SQL {idx}/{len(statements)}] SKIP(cleanup) {preview}")
            continue
        if cfg.circe_debug:
            preview = " ".join(stmt.strip().split())[:160]
            print(f"[Circe SQL {idx}/{len(statements)}] {preview}")

        if explain_dir is not None:
            normalized = " ".join(stmt.strip().split()).lower()
            is_ctas = normalized.startswith("create table ")
            if is_ctas and any(
                s in normalized
                for s in ("qualified_events", "final_cohort", "cohort_rows")
            ):
                m = re.search(r"\bAS\b", stmt, flags=re.IGNORECASE)
                if m:
                    select_part = stmt[m.end() :].strip()
                    if select_part.lower().startswith("select"):
                        try:
                            label = "circe"
                            if "qualified_events" in normalized:
                                label = "circe_qualified_events"
                            elif "final_cohort" in normalized:
                                label = "circe_final_cohort"
                            elif "cohort_rows" in normalized:
                                label = "circe_cohort_rows"
                            path = explain_dir / f"{explain_prefix}{label}.txt"
                            path.write_text(explain_formatted(con, select_part))
                        except Exception as e:
                            print(
                                f"Warning: failed to explain circe CTAS ({idx}): {e}",
                                file=sys.stderr,
                            )
        try:
            _exec_raw(con, stmt)
        except Exception as e:
            print(
                f"SQL Fail (stmt {idx}/{len(statements)}):\n{stmt[:400]}...",
                file=sys.stderr,
            )
            raise e
    sql_ms = (time.perf_counter() - sql_start) * 1000

    count_start = time.perf_counter()
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
                f"DELETE FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}",
            )
        except Exception:
            pass

    return row_count, {
        "sql_exec_ms": sql_ms,
        "count_query_ms": count_ms,
        "stage_tables": circe_stage_tables,
    }


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
    parser.add_argument("--no-python-stages", action="store_true")
    parser.add_argument(
        "--python-stages",
        action="store_true",
        help="Enable python stage materialization.",
    )
    parser.add_argument("--inline-python-codesets", action="store_true")
    parser.add_argument(
        "--python-materialize-codesets",
        action="store_true",
        help="Materialize codesets table (default).",
    )
    parser.add_argument(
        "--explain-dir",
        help="If set, write EXPLAIN FORMATTED output for python/circe into this directory.",
    )
    parser.add_argument(
        "--diff",
        action="store_true",
        help="Compute and print row-level diffs between Python and Circe cohorts.",
    )
    parser.add_argument(
        "--diff-limit",
        type=int,
        default=25,
        help="Max rows to print for each diff side.",
    )
    parser.add_argument(
        "--trace-stages",
        action="store_true",
        help="Keep intermediate Python/Circe tables and print per-stage counts (total + diff-subject subset).",
    )
    parser.add_argument(
        "--trace-subject-limit",
        type=int,
        default=50,
        help="Max number of subject_ids to include in per-stage subset counts.",
    )
    parser.add_argument(
        "--trace-subjects-from",
        choices=("both", "missing-in-circe", "missing-in-python"),
        default="both",
        help=(
            "When diffing, choose traced subject_ids from a specific diff side. "
            "'missing-in-circe' traces subjects present in Python but absent in Circe; "
            "'missing-in-python' traces subjects present in Circe but absent in Python."
        ),
    )
    parser.add_argument(
        "--diff-report",
        action="store_true",
        help="Print a copy/paste-friendly triage report for diffing subjects (implies --diff).",
    )
    return parser.parse_args()


def main():
    try:
        args = parse_args()
        if args.diff_report:
            args.diff = True
            args.trace_stages = True

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
        py_cfg = cfg
        if args.trace_stages:
            py_cfg = cfg.model_copy(
                update={
                    "python_materialize_stages": True,
                    "capture_stages": True,
                }
            )
        if (
            args.explain_dir
            and py_cfg.python_materialize_stages
            and not py_cfg.capture_stages
        ):
            # Enable stage SQL capture for explains without requiring python-stage-dir/debug-prefix.
            py_cfg = py_cfg.model_copy(update={"capture_stages": True})

        (
            py_sql,
            py_count,
            py_metrics,
            py_stages,
            py_ctx,
            py_diff_table,
            py_diff_db,
        ) = run_python_pipeline(
            con,
            py_cfg,
            keep_context_open=bool(args.explain_dir) or args.trace_stages,
            diff=args.diff,
        )
        if cfg.python_sql_out:
            cfg.python_sql_out.write_text(py_sql)
        if args.explain_dir:
            explain_dir = Path(args.explain_dir)
            explain_dir.mkdir(parents=True, exist_ok=True)
            try:
                explain_text = explain_formatted(con, py_sql)
                (explain_dir / "python_explain.txt").write_text(explain_text)
            except Exception as e:
                print(f"Warning: failed to explain python SQL: {e}", file=sys.stderr)
            if py_ctx is not None:
                try:
                    for idx, (table_name, statement) in enumerate(
                        py_ctx.captured_sql(), start=1
                    ):
                        safe_name = re.sub(r"[^A-Za-z0-9_]+", "_", table_name).strip(
                            "_"
                        )
                        path = explain_dir / f"python_stage_{idx:02d}_{safe_name}.txt"
                        try:
                            path.write_text(explain_formatted(con, statement))
                        except Exception as e:
                            print(
                                f"Warning: failed to explain python stage {table_name}: {e}",
                                file=sys.stderr,
                            )
                except Exception as e:
                    print(
                        f"Warning: failed to collect python stage SQL for explain: {e}",
                        file=sys.stderr,
                    )
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
        explain_dir = Path(args.explain_dir) if args.explain_dir else None
        if explain_dir is not None:
            explain_dir.mkdir(parents=True, exist_ok=True)

        circe_cfg = cfg
        if args.trace_stages:
            circe_cfg = cfg.model_copy(update={"cleanup_circe": False})
        if args.diff and cfg.cleanup_circe:
            circe_cfg = cfg.model_copy(update={"cleanup_circe": False})

        circe_count, circe_metrics = execute_circe_sql(
            con,
            circe_cfg,
            circe_sql,
            explain_dir=explain_dir,
            preserve_temp_tables=args.trace_stages,
        )

        diff_subject_ids: list[int] = []
        python_diff_qualified: str | None = None
        target_db = cfg.result_schema or cfg.cdm_schema
        circe_target_qualified = qualify_identifier_for_backend(
            cfg.cohort_table,
            target_db,
            cfg.backend,
        )

        if (
            (args.diff or args.trace_stages)
            and py_diff_table
            and py_diff_db is not None
        ):
            try:
                cohort_tbl = (
                    con.table(cfg.cohort_table, database=target_db)
                    if target_db
                    else con.table(cfg.cohort_table)
                )
                circe_rows = cohort_tbl.filter(
                    cohort_tbl.cohort_definition_id == cfg.cohort_id
                ).select(
                    cohort_definition_id=cohort_tbl.cohort_definition_id.cast("int64"),
                    subject_id=cohort_tbl.subject_id.cast("int64"),
                    cohort_start_date=cohort_tbl.cohort_start_date.cast("date"),
                    cohort_end_date=cohort_tbl.cohort_end_date.cast("date"),
                )
                py_rows_tbl = (
                    con.table(py_diff_table, database=py_diff_db)
                    if py_diff_db
                    else con.table(py_diff_table)
                )
                python_diff_qualified = qualify_identifier_for_backend(
                    py_diff_table,
                    py_diff_db,
                    cfg.backend,
                )
                py_rows = py_rows_tbl.select(
                    cohort_definition_id=py_rows_tbl.cohort_definition_id.cast("int64"),
                    subject_id=py_rows_tbl.subject_id.cast("int64"),
                    cohort_start_date=py_rows_tbl.cohort_start_date.cast("date"),
                    cohort_end_date=py_rows_tbl.cohort_end_date.cast("date"),
                )

                key = [
                    "cohort_definition_id",
                    "subject_id",
                    "cohort_start_date",
                    "cohort_end_date",
                ]
                missing_in_python = circe_rows.anti_join(py_rows, key)
                missing_in_circe = py_rows.anti_join(circe_rows, key)

                a = int(missing_in_python.count().execute())
                b = int(missing_in_circe.count().execute())
                if args.diff:
                    print(f"[Diff] missing_in_python={a} missing_in_circe={b}")
                    if args.diff_limit > 0 and a:
                        df = missing_in_python.limit(args.diff_limit).execute()
                        print(
                            f"[Diff] Examples missing in python (limit {args.diff_limit}):"
                        )
                        print(df)
                    if args.diff_limit > 0 and b:
                        df = missing_in_circe.limit(args.diff_limit).execute()
                        print(
                            f"[Diff] Examples missing in circe (limit {args.diff_limit}):"
                        )
                        print(df)

                if args.trace_stages and (a or b):
                    ids_a = (
                        missing_in_python.select(missing_in_python.subject_id)
                        .distinct()
                        .execute()
                    )
                    ids_b = (
                        missing_in_circe.select(missing_in_circe.subject_id)
                        .distinct()
                        .execute()
                    )
                    ids_missing_in_python = [
                        int(v) for v in ids_a["subject_id"].tolist()
                    ]
                    ids_missing_in_circe = [
                        int(v) for v in ids_b["subject_id"].tolist()
                    ]

                    if args.trace_subjects_from == "missing-in-circe":
                        diff_subject_ids = ids_missing_in_circe
                    elif args.trace_subjects_from == "missing-in-python":
                        diff_subject_ids = ids_missing_in_python
                    else:
                        diff_subject_ids = ids_missing_in_python + ids_missing_in_circe

                    diff_subject_ids = sorted(set(diff_subject_ids))[
                        : int(args.trace_subject_limit)
                    ]
            except Exception as e:
                print(f"Warning: failed to compute diffs: {e}", file=sys.stderr)
            finally:
                if not args.trace_stages:
                    try:
                        con.drop_table(py_diff_table, database=py_diff_db, force=True)
                    except Exception:
                        pass

        if args.diff and cfg.cleanup_circe and not args.trace_stages:
            try:
                qualified_table = qualify_identifier_for_backend(
                    cfg.cohort_table,
                    cfg.result_schema or cfg.cdm_schema,
                    cfg.backend,
                )
                _exec_raw(
                    con,
                    f"DELETE FROM {qualified_table} WHERE cohort_definition_id = {cfg.cohort_id}",
                )
            except Exception:
                pass

        if args.trace_stages:
            # Print stage counts for quick triage.
            print("[Trace] Circe stage tables:", circe_metrics.get("stage_tables", {}))
            if python_diff_qualified:
                print(f"[Trace] Python diff table: {python_diff_qualified}")
            print(f"[Trace] Circe target cohort table: {circe_target_qualified}")
            if py_ctx is not None:
                print("[Trace] Python stage tables:")
                stage_db = cfg.temp_schema
                for idx, (table_name, _statement) in enumerate(
                    py_ctx.captured_sql(), start=1
                ):
                    qualified = qualify_identifier_for_backend(
                        table_name,
                        stage_db,
                        cfg.backend,
                    )
                    total = _sql_count(con, qualified)
                    subset = (
                        _sql_count_for_ids(
                            con, qualified, id_column="person_id", ids=diff_subject_ids
                        )
                        if diff_subject_ids
                        else 0
                    )
                    suffix = f" subset(person_id)={subset}" if diff_subject_ids else ""
                    print(f"[Trace][Py {idx:02d}] {qualified} total={total}{suffix}")

            circe_tables = circe_metrics.get("stage_tables") or {}
            for label, name in circe_tables.items():
                # Circe names may already be qualified (esp. after Databricks rewriting).
                qualified = (
                    name
                    if "." in name or name.startswith(("`", '"'))
                    else qualify_identifier_for_backend(
                        name, cfg.temp_schema, cfg.backend
                    )
                )
                total = _sql_count(con, qualified)
                subset = (
                    _sql_count_for_ids(
                        con, qualified, id_column="person_id", ids=diff_subject_ids
                    )
                    if diff_subject_ids and label not in {"codesets"}
                    else 0
                )
                suffix = (
                    f" subset(person_id)={subset}"
                    if diff_subject_ids and label not in {"codesets"}
                    else ""
                )
                print(f"[Trace][Circe {label}] {qualified} total={total}{suffix}")

            if diff_subject_ids:
                print(
                    f"[Trace] Diff subject_ids ({len(diff_subject_ids)}): {diff_subject_ids[:20]}{'...' if len(diff_subject_ids) > 20 else ''}"
                )

                # Per-subject summaries at key stages.
                try:
                    expression = CohortExpression.model_validate_json(
                        cfg.json_path.read_text()
                    )
                    rule_names = [
                        r.name or f"rule_{i}"
                        for i, r in enumerate(expression.inclusion_rules or [], start=0)
                    ]

                    # Python: try to locate a few important stage tables by name.
                    py_tables = [
                        name
                        for (name, _stmt) in (py_ctx.captured_sql() if py_ctx else [])
                    ]

                    def _last_matching(substr: str) -> str | None:
                        for t in reversed(py_tables):
                            if substr in t:
                                return t
                        return None

                    py_primary = _last_matching("_stage_primary_events_")
                    py_src1 = _last_matching("_stage_primary_src_1_")
                    py_src2 = _last_matching("_stage_primary_src_2_")
                    py_src3 = _last_matching("_stage_primary_src_3_")
                    py_hits = _last_matching("_stage_inclusion_hits_")
                    py_inclusion = _last_matching("_stage_inclusion_")
                    if py_inclusion and "_stage_inclusion_hits_" in py_inclusion:
                        # pick the filtered inclusion stage if available
                        py_inclusion = _last_matching(
                            "_stage_inclusion_"
                        )  # best-effort
                    py_strategy = _last_matching("_stage_strategy_ends_")
                    py_censoring = _last_matching("_stage_censoring_")
                    py_final = _last_matching("_stage_final_cohort_")

                    if py_primary:
                        q = qualify_identifier_for_backend(
                            py_primary, cfg.temp_schema, cfg.backend
                        )
                        rows = _sql_person_summary(
                            con,
                            q,
                            id_column="person_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py primary_events] person_id, n, min_start, max_end:",
                            rows,
                        )

                    # Primary criteria source breakdown: which primary branch produced events for these subjects?
                    try:
                        if py_src1 or py_src2 or py_src3:
                            src_counts: dict[str, dict[int, int]] = {
                                "src1": {},
                                "src2": {},
                                "src3": {},
                            }
                            for label, table in (
                                ("src1", py_src1),
                                ("src2", py_src2),
                                ("src3", py_src3),
                            ):
                                if not table:
                                    continue
                                q = qualify_identifier_for_backend(
                                    table, cfg.temp_schema, cfg.backend
                                )
                                rows = _sql_person_summary(
                                    con,
                                    q,
                                    id_column="person_id",
                                    ids=diff_subject_ids,
                                )
                                src_counts[label] = {
                                    int(pid): int(n) for pid, n in rows
                                }

                            print(
                                "[Report][Py primary source breakdown] person_id\tprimary_src_1_n\tprimary_src_2_n\tprimary_src_3_n"
                            )
                            for pid in diff_subject_ids:
                                print(
                                    f"{pid}\t{src_counts['src1'].get(pid, 0)}\t{src_counts['src2'].get(pid, 0)}\t{src_counts['src3'].get(pid, 0)}"
                                )
                    except Exception as e:
                        print(
                            f"Warning: failed to compute python primary source breakdown: {e}",
                            file=sys.stderr,
                        )
                    if py_hits:
                        q = qualify_identifier_for_backend(
                            py_hits, cfg.temp_schema, cfg.backend
                        )
                        id_list = ", ".join(str(int(v)) for v in diff_subject_ids)
                        cols, rows = _sql_fetch_rows(
                            con,
                            f"""SELECT person_id, _rule_bit, COUNT(*) AS n
                                 FROM {q}
                                 WHERE person_id IN ({id_list})
                                 GROUP BY person_id, _rule_bit
                                 ORDER BY person_id, _rule_bit""",
                        )
                        decoded: list[tuple] = []
                        for person_id, bit, n in rows:
                            try:
                                idx = int(bit).bit_length() - 1
                            except Exception:
                                idx = None
                            name = (
                                rule_names[idx]
                                if idx is not None and 0 <= idx < len(rule_names)
                                else None
                            )
                            decoded.append((person_id, bit, idx, name, n))
                        print(
                            "[Report][Py inclusion_hits decoded] person_id\t_rule_bit\trule_index\trule_name\tn"
                        )
                        for r in decoded:
                            print("\t".join("" if v is None else str(v) for v in r))
                    if py_inclusion:
                        q = qualify_identifier_for_backend(
                            py_inclusion, cfg.temp_schema, cfg.backend
                        )
                        rows = _sql_person_summary(
                            con,
                            q,
                            id_column="person_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py inclusion] person_id, n, min_start, max_end:",
                            rows,
                        )
                    if py_strategy:
                        q = qualify_identifier_for_backend(
                            py_strategy, cfg.temp_schema, cfg.backend
                        )
                        rows = _sql_person_summary(
                            con,
                            q,
                            id_column="person_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py strategy_ends] person_id, n, min_start, max_end:",
                            rows,
                        )
                    if py_censoring:
                        q = qualify_identifier_for_backend(
                            py_censoring, cfg.temp_schema, cfg.backend
                        )
                        rows = _sql_person_summary(
                            con,
                            q,
                            id_column="person_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py censoring] person_id, n, min_start, max_end:",
                            rows,
                        )
                    if py_final:
                        q = qualify_identifier_for_backend(
                            py_final, cfg.temp_schema, cfg.backend
                        )
                        rows = _sql_person_summary(
                            con,
                            q,
                            id_column="person_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py final_cohort] person_id, n, min_start, max_end:",
                            rows,
                        )
                        cols, rows = _sql_fetch_rows(
                            con,
                            f"SELECT * FROM {q} WHERE person_id IN ({', '.join(str(int(v)) for v in diff_subject_ids)}) ORDER BY person_id, start_date",
                        )
                        print(
                            "[Report][Py final_cohort rows]\n"
                            + _format_rows_as_tsv(cols, rows)
                        )
                    if python_diff_qualified:
                        rows = _sql_person_summary(
                            con,
                            python_diff_qualified,
                            id_column="subject_id",
                            ids=diff_subject_ids,
                            extra_columns_sql=", MIN(cohort_start_date) AS min_start, MAX(cohort_end_date) AS max_end",
                        )
                        print(
                            "[Trace][Py cohort rows] subject_id, n, min_start, max_end:",
                            rows,
                        )
                        cols, rows = _sql_fetch_rows(
                            con,
                            f"SELECT * FROM {python_diff_qualified} WHERE subject_id IN ({', '.join(str(int(v)) for v in diff_subject_ids)}) ORDER BY subject_id, cohort_start_date",
                        )
                        print(
                            "[Report][Py cohort rows]\n"
                            + _format_rows_as_tsv(cols, rows)
                        )

                    # Circe: important stage tables
                    ct = circe_metrics.get("stage_tables") or {}
                    for label, id_col, extra in (
                        (
                            "qualified_events",
                            "person_id",
                            ", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        ),
                        (
                            "included_events",
                            "person_id",
                            ", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        ),
                        (
                            "final_cohort",
                            "person_id",
                            ", MIN(start_date) AS min_start, MAX(end_date) AS max_end",
                        ),
                    ):
                        if label in ct:
                            rows = _sql_person_summary(
                                con,
                                ct[label],
                                id_column=id_col,
                                ids=diff_subject_ids,
                                extra_columns_sql=extra,
                            )
                            print(
                                f"[Trace][Circe {label}] person_id, n, min_start, max_end:",
                                rows,
                            )
                    if "inclusion_events" in ct:
                        id_list = ", ".join(str(int(v)) for v in diff_subject_ids)
                        cols, rows = _sql_fetch_rows(
                            con,
                            f"""SELECT person_id, inclusion_rule_id, COUNT(*) AS n
                                 FROM {ct["inclusion_events"]}
                                 WHERE person_id IN ({id_list})
                                 GROUP BY person_id, inclusion_rule_id
                                 ORDER BY person_id, inclusion_rule_id""",
                        )
                        print(
                            "[Report][Circe inclusion_events] "
                            + _format_rows_as_tsv(cols, rows)
                        )
                    rows = _sql_person_summary(
                        con,
                        circe_target_qualified,
                        id_column="subject_id",
                        ids=diff_subject_ids,
                        extra_columns_sql=", MIN(cohort_start_date) AS min_start, MAX(cohort_end_date) AS max_end",
                    )
                    print(
                        "[Trace][Circe cohort rows] subject_id, n, min_start, max_end:",
                        rows,
                    )
                    cols, rows = _sql_fetch_rows(
                        con,
                        f"SELECT * FROM {circe_target_qualified} WHERE cohort_definition_id = {int(cfg.cohort_id)} AND subject_id IN ({', '.join(str(int(v)) for v in diff_subject_ids)}) ORDER BY subject_id, cohort_start_date",
                    )
                    print(
                        "[Report][Circe cohort rows]\n"
                        + _format_rows_as_tsv(cols, rows)
                    )
                except Exception as e:
                    print(
                        f"Warning: failed to print per-subject summaries: {e}",
                        file=sys.stderr,
                    )

        if args.diff_report and py_ctx is not None:
            # Clean up python stage tables and diff table; report mode should leave nothing behind.
            stage_db = cfg.temp_schema
            for table_name, _stmt in py_ctx.captured_sql():
                try:
                    con.drop_table(table_name, database=stage_db, force=True)
                except Exception:
                    pass
            if py_diff_table and py_diff_db is not None:
                try:
                    con.drop_table(py_diff_table, database=py_diff_db, force=True)
                except Exception:
                    pass
            try:
                py_ctx.close()
            except Exception:
                pass

        if args.diff_report:
            # Clean up Circe stage tables we preserved.
            stage_tables = circe_metrics.get("stage_tables") or {}
            for _label, qualified in stage_tables.items():
                try:
                    _exec_raw(con, f"DROP TABLE IF EXISTS {qualified}")
                except Exception:
                    pass
            # Clear the target cohort table for this cohort id (Circe rows).
            try:
                _exec_raw(
                    con,
                    f"DELETE FROM {circe_target_qualified} WHERE cohort_definition_id = {cfg.cohort_id}",
                )
            except Exception:
                pass

        if py_ctx is not None and not args.trace_stages:
            py_ctx.close()

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
