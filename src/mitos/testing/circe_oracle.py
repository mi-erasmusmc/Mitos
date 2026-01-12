from __future__ import annotations

import contextlib
import shutil
import subprocess
import tempfile
import textwrap
import time
from csv import DictWriter
from dataclasses import dataclass
from pathlib import Path

import ibis

from mitos.sql_split import split_sql_statements


@dataclass(frozen=True)
class CirceSqlConfig:
    json_path: Path
    cdm_schema: str
    vocab_schema: str
    result_schema: str
    target_schema: str
    target_table: str
    cohort_id: int
    temp_schema: str | None = None
    target_dialect: str = "duckdb"
    rscript_path: str | None = None


def generate_circe_sql_via_r(cfg: CirceSqlConfig) -> tuple[str, float]:
    rscript_exe = cfg.rscript_path or shutil.which("Rscript")
    if not rscript_exe:
        raise RuntimeError(
            "Rscript executable not found. Install R and ensure `Rscript` is on PATH."
        )

    temp_arg = cfg.temp_schema or ""

    tmp_dir = Path(tempfile.mkdtemp(prefix="mitos_fieldcases_circe_"))
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
        json_str <- paste(readLines(json_path, warn = FALSE), collapse = "\n")
        expression <- CirceR::cohortExpressionFromJson(json_str)
        options <- CirceR::createGenerateOptions()
        options$generateStats <- FALSE; options$useTempTables <- FALSE;
        options$tempEmulationSchema <- if(temp_schema=="") NULL else temp_schema

        sql <- CirceR::buildCohortQuery(expression, options)
        sql <- SqlRender::render(
          sql,
          cdm_database_schema=cdm_schema,
          vocabulary_database_schema=vocab_schema,
          results_database_schema=result_schema,
          target_database_schema=target_schema,
          target_cohort_table=target_table,
          target_cohort_id=cohort_id,
          tempEmulationSchema=if(temp_schema=="") NULL else temp_schema
        )
        translated <- SqlRender::translate(sql=sql, targetDialect=target_dialect)
        writeLines(translated, out_path, useBytes = TRUE)
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
        cfg.result_schema,
        cfg.target_schema,
        cfg.target_table,
        str(cfg.cohort_id),
        temp_arg,
        cfg.target_dialect,
        str(out_path),
    ]

    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - start) * 1000
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        raise RuntimeError(
            "Circe R Error.\n"
            f"returncode={result.returncode}\n"
            f"stderr:\n{stderr or '<empty>'}\n"
            f"stdout:\n{stdout or '<empty>'}\n"
        )

    try:
        sql_text = out_path.read_text(encoding="utf-8", errors="replace").strip()
    finally:
        with contextlib.suppress(Exception):
            out_path.unlink()
        with contextlib.suppress(Exception):
            script_path.unlink()
        with contextlib.suppress(Exception):
            shutil.rmtree(tmp_dir, ignore_errors=True)

    if not sql_text:
        raise RuntimeError("Circe SQL generation returned empty SQL.")

    return sql_text, elapsed


def generate_circe_sql_batch_via_r(
    cfgs: dict[str, CirceSqlConfig],
) -> tuple[dict[str, str], float]:
    """
    Generate Circe SQL for many cohort JSONs in a single Rscript invocation.

    This is significantly faster than calling `generate_circe_sql_via_r` per case because it
    avoids paying the R + Java startup cost N times.
    """
    if not cfgs:
        return {}, 0.0

    # Use the first config to resolve Rscript path; allow per-cfg override but keep it simple.
    first = next(iter(cfgs.values()))
    rscript_exe = first.rscript_path or shutil.which("Rscript")
    if not rscript_exe:
        raise RuntimeError(
            "Rscript executable not found. Install R and ensure `Rscript` is on PATH."
        )

    tmp_dir = Path(tempfile.mkdtemp(prefix="mitos_fieldcases_circe_batch_"))
    jobs_path = tmp_dir / "jobs.csv"
    script_path = tmp_dir / "circe_batch.R"

    # Write job CSV.
    fieldnames = [
        "name",
        "json_path",
        "cdm_schema",
        "vocab_schema",
        "result_schema",
        "target_schema",
        "target_table",
        "cohort_id",
        "temp_schema",
        "target_dialect",
        "out_path",
    ]
    with jobs_path.open("w", encoding="utf-8", newline="") as f:
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for name, cfg in cfgs.items():
            out_path = tmp_dir / f"{name}.sql"
            writer.writerow(
                {
                    "name": name,
                    "json_path": str(cfg.json_path),
                    "cdm_schema": cfg.cdm_schema,
                    "vocab_schema": cfg.vocab_schema,
                    "result_schema": cfg.result_schema,
                    "target_schema": cfg.target_schema,
                    "target_table": cfg.target_table,
                    "cohort_id": str(cfg.cohort_id),
                    "temp_schema": cfg.temp_schema or "",
                    "target_dialect": cfg.target_dialect,
                    "out_path": str(out_path),
                }
            )

    r_script = textwrap.dedent(
        r"""
        suppressPackageStartupMessages({library(CirceR); library(SqlRender)})
        args <- commandArgs(trailingOnly = TRUE)
        jobs_path <- args[[1]]

        jobs <- read.csv(jobs_path, stringsAsFactors = FALSE)
        if ("temp_schema" %in% names(jobs)) {
          jobs$temp_schema[is.na(jobs$temp_schema)] <- ""
        }

        for (i in 1:nrow(jobs)) {
          json_str <- paste(readLines(jobs$json_path[i], warn = FALSE), collapse = "\n")
          expression <- CirceR::cohortExpressionFromJson(json_str)
          options <- CirceR::createGenerateOptions()
          options$generateStats <- FALSE; options$useTempTables <- FALSE;
          temp_schema <- jobs$temp_schema[i]
          options$tempEmulationSchema <- if(temp_schema=="") NULL else temp_schema

          sql <- CirceR::buildCohortQuery(expression, options)
          sql <- SqlRender::render(
            sql,
            cdm_database_schema=jobs$cdm_schema[i],
            vocabulary_database_schema=jobs$vocab_schema[i],
            results_database_schema=jobs$result_schema[i],
            target_database_schema=jobs$target_schema[i],
            target_cohort_table=jobs$target_table[i],
            target_cohort_id=as.integer(jobs$cohort_id[i]),
            tempEmulationSchema=if(temp_schema=="") NULL else temp_schema
          )
          translated <- SqlRender::translate(sql=sql, targetDialect=jobs$target_dialect[i])
          writeLines(translated, jobs$out_path[i], useBytes = TRUE)
        }
        """
    ).strip()
    script_path.write_text(r_script, encoding="utf-8")

    cmd = [
        rscript_exe,
        "--vanilla",
        str(script_path),
        str(jobs_path),
    ]

    start = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - start) * 1000
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        raise RuntimeError(
            "Circe R batch error.\n"
            f"returncode={result.returncode}\n"
            f"stderr:\n{stderr or '<empty>'}\n"
            f"stdout:\n{stdout or '<empty>'}\n"
        )

    try:
        sql_by_name: dict[str, str] = {}
        for name in cfgs.keys():
            out_path = tmp_dir / f"{name}.sql"
            sql_text = out_path.read_text(encoding="utf-8", errors="replace").strip()
            if not sql_text:
                raise RuntimeError(f"Circe batch SQL generation produced empty SQL for: {name}")
            sql_by_name[name] = sql_text
        return sql_by_name, elapsed
    finally:
        with contextlib.suppress(Exception):
            shutil.rmtree(tmp_dir, ignore_errors=True)


def execute_circe_sql(con: ibis.BaseBackend, sql_script: str) -> None:
    for stmt in split_sql_statements(sql_script):
        # Circe SQL may include trailing semicolons in comments; split strips statement terminators.
        con.raw_sql(stmt)
