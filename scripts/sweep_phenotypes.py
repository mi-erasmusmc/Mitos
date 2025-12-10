#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.compare_cohort_counts import (
    execute_circe_sql,
    generate_circe_sql_via_r,
    run_python_pipeline,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run all phenotype fixtures against DuckDB and capture row-count parity stats."
    )
    parser.add_argument(
        "--phenotype-dir",
        default="fixtures/phenotypes",
        help="Directory containing phenotype JSON files.",
    )
    parser.add_argument(
        "--cdm-db",
        default="duckyS_local.duckdb",
        help="DuckDB database file with the OMOP CDM.",
    )
    parser.add_argument(
        "--cdm-schema",
        default="main",
        help="CDM schema name within the DuckDB database.",
    )
    parser.add_argument(
        "--vocab-schema",
        help="Vocabulary schema name (defaults to the CDM schema).",
    )
    parser.add_argument(
        "--result-schema",
        help="Schema that stores the Circe target cohort table (defaults to the CDM schema).",
    )
    parser.add_argument(
        "--target-table",
        default="circe_cohort",
        help="Writable cohort results table name.",
    )
    parser.add_argument(
        "--base-cohort-id",
        type=int,
        default=1000,
        help="Base cohort_definition_id; an offset is added per phenotype to avoid collisions.",
    )
    parser.add_argument(
        "--output",
        default="report_sweep.json",
        help="Path to write the summary JSON report.",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        help="Optional DuckDB memory limit (e.g., 24GB).",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        help="Optional DuckDB thread count override.",
    )
    parser.add_argument(
        "--duckdb-preserve-insertion-order",
        choices=["true", "false"],
        help="Set DuckDB preserve_insertion_order.",
    )
    parser.add_argument(
        "--duckdb-temp-dir",
        help="Optional DuckDB temp_directory path for spills.",
    )
    parser.add_argument(
        "--only-mismatches-from",
        help="When provided, only phenotypes marked as mismatches in the given report JSON are re-run.",
    )
    parser.add_argument(
        "--skip-existing-from",
        help="Skip phenotypes already present (any status) in the given report JSON file.",
    )
    return parser.parse_args()


def build_duckdb_config(args: argparse.Namespace) -> dict[str, str] | None:
    config: dict[str, str] = {}
    if args.duckdb_memory_limit:
        config["memory_limit"] = args.duckdb_memory_limit
    if args.duckdb_threads:
        config["threads"] = str(args.duckdb_threads)
    if args.duckdb_preserve_insertion_order:
        config["preserve_insertion_order"] = args.duckdb_preserve_insertion_order
    if args.duckdb_temp_dir:
        config["temp_directory"] = args.duckdb_temp_dir
    return config or None


def load_mismatch_paths(report_path: Path, phenotype_dir: Path) -> list[Path]:
    if not report_path.exists():
        raise FileNotFoundError(f"Mismatch report not found: {report_path}")
    with report_path.open() as f:
        records = json.load(f)
    mismatch_paths: list[Path] = []
    for record in records:
        if record.get("status") != "mismatch":
            continue
        json_path = record.get("json_path")
        if not json_path:
            continue
        path = Path(json_path)
        if not path.is_absolute():
            path = REPO_ROOT / path
        path = path.resolve()
        if not path.exists():
            print(f"  Skipping missing mismatch path: {path}", flush=True)
            continue
        if phenotype_dir not in path.parents and path != phenotype_dir:
            print(f"  Skipping mismatch outside phenotype dir: {path}", flush=True)
            continue
        mismatch_paths.append(path)
    return sorted(set(mismatch_paths))


def load_existing_paths(report_path: Path, phenotype_dir: Path) -> set[Path]:
    if not report_path.exists():
        raise FileNotFoundError(f"Existing report not found: {report_path}")
    with report_path.open() as f:
        records = json.load(f)
    existing: set[Path] = set()
    for record in records:
        json_path = record.get("json_path")
        if not json_path:
            continue
        path = Path(json_path)
        if not path.is_absolute():
            path = REPO_ROOT / path
        path = path.resolve()
        if not path.exists():
            continue
        if phenotype_dir not in path.parents and path != phenotype_dir:
            continue
        existing.add(path)
    return existing


def main() -> int:
    args = parse_args()
    phenotype_dir = Path(args.phenotype_dir).resolve()
    if not phenotype_dir.exists():
        print(f"Phenotype directory not found: {phenotype_dir}", file=sys.stderr)
        return 1
    json_paths: list[Path]
    if args.only_mismatches_from:
        mismatch_report = Path(args.only_mismatches_from)
        try:
            json_paths = load_mismatch_paths(mismatch_report, phenotype_dir)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if not json_paths:
            print(f"No mismatches found in {mismatch_report}; nothing to run.", file=sys.stderr)
            return 0
    else:
        json_paths = sorted(phenotype_dir.glob("*.json"))
    if not json_paths:
        print(f"No cohort JSON files found under {phenotype_dir}", file=sys.stderr)
        return 1

    if args.skip_existing_from:
        existing_report = Path(args.skip_existing_from)
        try:
            existing_paths = load_existing_paths(existing_report, phenotype_dir)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        if existing_paths:
            filtered = [path for path in json_paths if path.resolve() not in existing_paths]
            skipped = len(json_paths) - len(filtered)
            json_paths = filtered
            if skipped:
                print(f"Skipping {skipped} phenotype(s) already present in {existing_report}", flush=True)
        if not json_paths:
            print("All phenotypes were already present in the existing report; nothing to run.", flush=True)
            return 0

    duckdb_config = build_duckdb_config(args)
    vocab_schema = args.vocab_schema or args.cdm_schema
    result_schema = args.result_schema or args.cdm_schema

    summary: list[dict[str, Any]] = []
    mismatches = 0
    failures = 0

    total = len(json_paths)
    for idx, json_path in enumerate(json_paths, start=1):
        label = json_path.stem
        cohort_id = args.base_cohort_id + idx
        print(f"[{idx}/{total}] {label} (cohort_id={cohort_id}) ...", flush=True)
        record: dict[str, Any] = {
            "phenotype": label,
            "json_path": str(json_path),
            "cohort_id": cohort_id,
        }

        python_count = None
        circe_count = None

        try:
            _, python_count, python_metrics, _ = run_python_pipeline(
                json_path=json_path,
                db_path=args.cdm_db,
                cdm_schema=args.cdm_schema,
                vocab_schema=vocab_schema,
                duckdb_config=dict(duckdb_config) if duckdb_config else None,
                capture_stages=False,
            )
            record["python_rows"] = python_count
            record["python_total_ms"] = python_metrics.get("total_ms")
        except Exception as exc:
            failures += 1
            record["python_error"] = str(exc)
            print(f"  Python pipeline failed: {exc}", flush=True)
        else:
            python_count = int(python_count)

        try:
            circe_sql, circe_generate_ms = generate_circe_sql_via_r(
                json_path=json_path,
                cdm_schema=args.cdm_schema,
                vocab_schema=vocab_schema,
                result_schema=result_schema,
                target_schema=result_schema,
                target_table=args.target_table,
                cohort_id=cohort_id,
                temp_schema=result_schema,
            )
            circe_count, circe_exec_metrics = execute_circe_sql(
                sql=circe_sql,
                db_path=args.cdm_db,
                result_schema=result_schema,
                target_table=args.target_table,
                cohort_id=cohort_id,
                temp_schema=result_schema,
                duckdb_config=dict(duckdb_config) if duckdb_config else None,
            )
            record["circe_rows"] = circe_count
            record["circe_generate_ms"] = circe_generate_ms
            record["circe_sql_exec_ms"] = circe_exec_metrics.get("sql_exec_ms")
            record["circe_count_query_ms"] = circe_exec_metrics.get("count_query_ms")
            record["circe_total_ms"] = circe_generate_ms + circe_exec_metrics.get("sql_exec_ms", 0.0)
        except Exception as exc:
            failures += 1
            record["circe_error"] = str(exc)
            print(f"  Circe pipeline failed: {exc}", flush=True)
        else:
            circe_count = int(circe_count)

        if python_count is not None and circe_count is not None:
            diff = python_count - circe_count
            record["row_diff"] = diff
            record["status"] = "match" if diff == 0 else "mismatch"
            if diff != 0:
                mismatches += 1
                print(f"  Row mismatch: python={python_count} circe={circe_count} (diff={diff})", flush=True)
            else:
                print(f"  Row match: {python_count}", flush=True)
        else:
            record["status"] = "error"

        summary.append(record)

    output_path = Path(args.output)
    output_path.write_text(json.dumps(summary, indent=2))
    print(
        f"\nCompleted {total} phenotypes. "
        f"Mismatches: {mismatches}, Failures: {failures}. "
        f"Report written to {output_path}",
        flush=True,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
