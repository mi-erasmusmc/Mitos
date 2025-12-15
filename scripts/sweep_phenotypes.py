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

from scripts.compare_cohort_counts import (  # noqa: E402
    ProfilesFile,
    get_connection,
    get_ohdsi_dialect,
    load_yaml_with_env,
    execute_circe_sql,
    explain_formatted,
    generate_circe_sql_via_r,
    run_python_pipeline,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep phenotypes and compare row counts (python vs circe).")
    parser.add_argument("--config", default="profiles.yaml")
    parser.add_argument("--profile", required=True)

    parser.add_argument(
        "--phenotype-dir",
        default="cohorts",
        help="Directory containing phenotype JSON files.",
    )
    parser.add_argument(
        "--pattern",
        default="phenotype-*.json",
        help="Glob pattern under phenotype-dir.",
    )
    parser.add_argument(
        "--phenotypes",
        nargs="*",
        help="Optional explicit list of phenotype JSON filenames under phenotype-dir.",
    )

    parser.add_argument(
        "--target-table",
        help="Writable cohort results table name (overrides profile).",
    )
    parser.add_argument(
        "--result-schema",
        help="Writable catalog.schema for cohort results (overrides profile).",
    )
    parser.add_argument(
        "--temp-schema",
        help="Writable catalog.schema for Circe temp emulation (overrides profile).",
    )
    parser.add_argument(
        "--cdm-schema",
        help="Readable catalog.schema for CDM (overrides profile).",
    )
    parser.add_argument(
        "--vocab-schema",
        help="Readable catalog.schema for vocab (overrides profile).",
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
        "--only-mismatches-from",
        help="Only re-run phenotypes marked mismatched in a prior report JSON.",
    )
    parser.add_argument(
        "--skip-existing-from",
        help="Skip phenotypes already present (any status) in an existing report JSON.",
    )

    # Defaults requested: no python staging, inline codesets
    parser.add_argument("--python-stages", action="store_true", help="Enable python stage materialization.")
    parser.add_argument(
        "--inline-python-codesets",
        action="store_true",
        help="Inline codesets instead of materializing (materialize is default).",
    )
    parser.add_argument(
        "--python-materialize-codesets",
        action="store_true",
        help="Materialize codesets table (default).",
    )
    parser.add_argument("--circe-debug", action="store_true")
    parser.add_argument("--no-cleanup-circe", action="store_true")
    parser.add_argument("--explain-dir", help="Write EXPLAIN FORMATTED outputs per phenotype.")
    return parser.parse_args()


def load_paths_from_report(report_path: Path, phenotype_dir: Path, *, status: str | None) -> list[Path]:
    if not report_path.exists():
        raise FileNotFoundError(f"Report not found: {report_path}")
    with report_path.open() as f:
        records = json.load(f)
    paths: list[Path] = []
    for record in records:
        if status is not None and record.get("status") != status:
            continue
        json_path = record.get("json_path")
        if not json_path:
            continue
        path = Path(json_path)
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        if not path.exists():
            continue
        if phenotype_dir not in path.parents and path != phenotype_dir:
            continue
        paths.append(path)
    return sorted(set(paths))


def main() -> int:
    args = parse_args()
    phenotype_dir = Path(args.phenotype_dir).resolve()
    if not phenotype_dir.exists():
        print(f"Phenotype directory not found: {phenotype_dir}", file=sys.stderr)
        return 1

    raw = load_yaml_with_env(args.config)
    profiles = ProfilesFile(**raw)
    if args.profile not in profiles.profiles:
        print(f"Profile '{args.profile}' not found in {args.config}", file=sys.stderr)
        return 1
    cfg = profiles.profiles[args.profile]
    print(f"Using profile: {args.profile}")

    if args.only_mismatches_from:
        json_paths = load_paths_from_report(Path(args.only_mismatches_from), phenotype_dir, status="mismatch")
    elif args.phenotypes:
        json_paths = [(phenotype_dir / name).resolve() for name in args.phenotypes]
    else:
        json_paths = sorted(phenotype_dir.glob(args.pattern))

    json_paths = [p for p in json_paths if p.exists()]
    if not json_paths:
        print(f"No phenotype JSON files found under {phenotype_dir} (pattern={args.pattern})", file=sys.stderr)
        return 1

    if args.skip_existing_from:
        existing_paths = set(load_paths_from_report(Path(args.skip_existing_from), phenotype_dir, status=None))
        before = len(json_paths)
        json_paths = [p for p in json_paths if p.resolve() not in existing_paths]
        skipped = before - len(json_paths)
        if skipped:
            print(f"Skipping {skipped} phenotype(s) already present in {args.skip_existing_from}", flush=True)
        if not json_paths:
            print("All phenotypes were already present in the existing report; nothing to run.", flush=True)
            return 0

    # Apply global overrides once; per-phenotype settings are applied via model_copy below.
    overrides: dict[str, Any] = {}
    if args.target_table:
        overrides["cohort_table"] = args.target_table
    if args.result_schema:
        overrides["result_schema"] = args.result_schema
    if args.temp_schema:
        overrides["temp_schema"] = args.temp_schema
    if args.cdm_schema:
        overrides["cdm_schema"] = args.cdm_schema
    if args.vocab_schema:
        overrides["vocab_schema"] = args.vocab_schema
    if args.circe_debug:
        overrides["circe_debug"] = True
    if args.no_cleanup_circe:
        overrides["cleanup_circe"] = False

    # Defaults: no python staging; do materialize codesets (override with --inline-python-codesets).
    overrides["python_materialize_stages"] = bool(args.python_stages)
    overrides["python_materialize_codesets"] = not bool(args.inline_python_codesets)

    cfg = cfg.model_copy(update=overrides)

    con = get_connection(cfg)
    dialect = get_ohdsi_dialect(con)
    print(f"Dialect: {dialect} | Backend: {cfg.backend}")

    summary: list[dict[str, Any]] = []
    mismatches = 0
    failures = 0
    total = len(json_paths)
    explain_dir: Path | None = None
    if args.explain_dir:
        explain_dir = Path(args.explain_dir)
        explain_dir.mkdir(parents=True, exist_ok=True)

    try:
        for idx, json_path in enumerate(json_paths, start=1):
            label = json_path.stem
            cohort_id = args.base_cohort_id + idx
            print(f"[{idx}/{total}] {label} (cohort_id={cohort_id}) ...", flush=True)

            per = cfg.model_copy(update={"json_path": json_path, "cohort_id": cohort_id})
            # Ensure Circe temp emulation defaults to result_schema if not set.
            if getattr(per, "temp_schema", None) is None:
                per = per.model_copy(update={"temp_schema": per.result_schema})

            record: dict[str, Any] = {
                "phenotype": label,
                "json_path": str(json_path),
                "cohort_id": cohort_id,
            }

            python_count = None
            circe_count = None

            try:
                py_cfg = per
                if explain_dir is not None and per.python_materialize_stages and not per.capture_stages:
                    py_cfg = per.model_copy(update={"capture_stages": True})

                py_sql, python_count, python_metrics, _, py_ctx = run_python_pipeline(
                    con, py_cfg, keep_context_open=bool(explain_dir)
                )
                record["python_rows"] = int(python_count)
                record["python_total_ms"] = python_metrics.get("total_ms")
                if explain_dir is not None:
                    try:
                        (explain_dir / f"{label}_python_explain.txt").write_text(
                            explain_formatted(con, py_sql)
                        )
                    except Exception as exc:
                        record["python_explain_error"] = str(exc)
                    finally:
                        if py_ctx is not None:
                            py_ctx.close()
            except Exception as exc:
                failures += 1
                record["python_error"] = str(exc)
                print(f"  Python pipeline failed: {exc}", flush=True)

            try:
                circe_sql, circe_generate_ms = generate_circe_sql_via_r(per, dialect)
                circe_count, circe_exec_metrics = execute_circe_sql(
                    con,
                    per,
                    circe_sql,
                    explain_dir=explain_dir,
                    explain_prefix=f"{label}_",
                )
                record["circe_rows"] = int(circe_count)
                record["circe_generate_ms"] = circe_generate_ms
                record["circe_sql_exec_ms"] = circe_exec_metrics.get("sql_exec_ms")
                record["circe_count_query_ms"] = circe_exec_metrics.get("count_query_ms")
                record["circe_total_ms"] = circe_generate_ms + circe_exec_metrics.get("sql_exec_ms", 0.0)
            except Exception as exc:
                failures += 1
                record["circe_error"] = str(exc)
                print(f"  Circe pipeline failed: {exc}", flush=True)

            if record.get("python_rows") is not None and record.get("circe_rows") is not None:
                diff = int(record["python_rows"]) - int(record["circe_rows"])
                record["row_diff"] = diff
                record["status"] = "match" if diff == 0 else "mismatch"
                if diff != 0:
                    mismatches += 1
                    print(
                        f"  Row mismatch: python={record['python_rows']} circe={record['circe_rows']} (diff={diff})",
                        flush=True,
                    )
                else:
                    print(f"  Row match: {record['python_rows']}", flush=True)
            else:
                record["status"] = "error"

            summary.append(record)
    finally:
        if hasattr(con, "close"):
            con.close()

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
