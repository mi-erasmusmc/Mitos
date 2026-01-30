#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.compare_cohort_counts import (  # noqa: E402
    ProfilesFile,
    get_connection,
    load_yaml_with_env,
    run_python_pipeline,
)


@dataclass(frozen=True)
class ReferenceRow:
    phenotype: str
    json_path: str | None
    circe_rows: int | None


def _load_reference_rows(path: Path) -> list[ReferenceRow]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Reference report must be a list of objects: {path}")
    out: list[ReferenceRow] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        phenotype = str(item.get("phenotype") or "")
        if not phenotype:
            continue
        circe_rows = item.get("circe_rows")
        out.append(
            ReferenceRow(
                phenotype=phenotype,
                json_path=item.get("json_path"),
                circe_rows=(int(circe_rows) if circe_rows is not None else None),
            )
        )
    return out


def _resolve_cohort_json(
    *,
    reference: ReferenceRow,
    phenotype_dir: Path,
) -> Path:
    # 1) Prefer the absolute path stored in the reference report (works on the same machine).
    if reference.json_path:
        p = Path(reference.json_path)
        if p.exists():
            return p.resolve()

    # 2) Common case: phenotype label maps to phenotype-{id}.json under phenotype_dir.
    candidate = (phenotype_dir / f"{reference.phenotype}.json").resolve()
    if candidate.exists():
        return candidate

    # 3) Fallback: if phenotype is already a filename, try it directly.
    if reference.phenotype.endswith(".json"):
        candidate = (phenotype_dir / reference.phenotype).resolve()
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not resolve cohort JSON for "
        f"phenotype={reference.phenotype!r} (reference json_path={reference.json_path!r}). "
        f"Tried under phenotype_dir={phenotype_dir}."
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Run Mitos against a backend and compare row counts to a reference sweep report "
            "(e.g. DuckDB/Circe counts), without requiring CirceR on the target system."
        )
    )
    p.add_argument(
        "--config",
        default="profiles.yaml",
        help="Path to profiles.yaml (read from CWD by default).",
    )
    p.add_argument(
        "--profile",
        required=True,
        help="Profile name in profiles.yaml to use for execution.",
    )

    p.add_argument(
        "--reference-report",
        required=True,
        help="Path to a reference sweep JSON (list of rows including phenotype + circe_rows).",
    )
    p.add_argument(
        "--reference-count-field",
        default="circe_rows",
        choices=("circe_rows",),
        help="Which reference count to compare against (default: circe_rows).",
    )

    p.add_argument(
        "--phenotype-dir",
        default="cohorts",
        help="Directory containing phenotype JSON files (used to resolve JSON paths).",
    )
    p.add_argument(
        "--phenotypes",
        nargs="*",
        help="Optional explicit list of phenotype basenames (e.g. phenotype-10.json) to run.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on how many phenotypes from the reference report to run.",
    )
    p.add_argument(
        "--output",
        default="report_sweep_postgres.json",
        help="Path to write the output JSON report.",
    )
    p.add_argument(
        "--python-stages",
        action="store_true",
        help="Enable python stage materialization.",
    )
    p.add_argument(
        "--inline-python-codesets",
        action="store_true",
        help="Inline codesets instead of materializing (materialize is default).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    phenotype_dir = Path(args.phenotype_dir).resolve()
    if not phenotype_dir.exists():
        raise FileNotFoundError(f"Phenotype directory not found: {phenotype_dir}")

    reference_path = Path(args.reference_report).resolve()
    reference_rows = _load_reference_rows(reference_path)

    if args.phenotypes:
        wanted: set[str] = set()
        for name in args.phenotypes:
            # allow passing either phenotype-10 or phenotype-10.json
            name = name.strip()
            if not name:
                continue
            if name.endswith(".json"):
                wanted.add(name.removesuffix(".json"))
            else:
                wanted.add(name)
        reference_rows = [r for r in reference_rows if r.phenotype in wanted]

    if args.limit is not None:
        reference_rows = reference_rows[: int(args.limit)]

    config_data = load_yaml_with_env(args.config)
    profiles_file = ProfilesFile.model_validate(config_data)
    if args.profile not in profiles_file.profiles:
        raise KeyError(f"Profile not found in {args.config}: {args.profile}")
    base_cfg = profiles_file.profiles[args.profile]
    python_materialize_stages = bool(
        getattr(base_cfg, "python_materialize_stages", False)
    ) or bool(args.python_stages)
    python_materialize_codesets = bool(
        getattr(base_cfg, "python_materialize_codesets", True)
    )
    if args.inline_python_codesets:
        python_materialize_codesets = False

    con = get_connection(base_cfg)
    try:
        total = len(reference_rows)
        mismatches = 0
        failures = 0
        no_reference = 0

        print(
            f"Reference: {reference_path} | count_field={args.reference_count_field} | cohorts={total}",
            flush=True,
        )

        results: list[dict[str, Any]] = []
        for idx, ref in enumerate(reference_rows):
            record: dict[str, Any] = {
                "phenotype": ref.phenotype,
                "reference_report": str(reference_path),
                "reference_count_field": args.reference_count_field,
                "reference_circe_rows": ref.circe_rows,
                "reference_json_path": ref.json_path,
            }

            print(f"[{idx + 1}/{total}] {ref.phenotype}", flush=True)

            try:
                json_path = _resolve_cohort_json(
                    reference=ref, phenotype_dir=phenotype_dir
                )
                record["json_path"] = str(json_path)

                cfg = base_cfg.model_copy(
                    update={
                        "json_path": json_path,
                        # Keep cohort_id stable but unique-ish if the backend writes anywhere.
                        "cohort_id": int(getattr(base_cfg, "cohort_id", 1)) + idx,
                        "python_materialize_stages": python_materialize_stages,
                        "python_materialize_codesets": python_materialize_codesets,
                    }
                )

                _sql, python_count, metrics, _stages, _ctx, _diff_table, _diff_db = (
                    run_python_pipeline(con, cfg, keep_context_open=False, diff=False)
                )
                record["python_rows"] = int(python_count)
                record["python_total_ms"] = metrics.get("total_ms")
                record["python_build_ms"] = metrics.get("build_exec_ms")
                record["python_codeset_ms"] = metrics.get("codeset_exec_ms")
                record["python_final_exec_ms"] = metrics.get("final_exec_ms")

                if ref.circe_rows is None:
                    record["status"] = "no_reference"
                    no_reference += 1
                    print("  No reference count available.", flush=True)
                else:
                    diff = int(record["python_rows"]) - int(ref.circe_rows)
                    record["row_diff"] = diff
                    record["status"] = "match" if diff == 0 else "mismatch"
                    if diff == 0:
                        print(f"  Row match: {record['python_rows']}", flush=True)
                    else:
                        mismatches += 1
                        print(
                            f"  Row mismatch: python={record['python_rows']} reference={ref.circe_rows} (diff={diff})",
                            flush=True,
                        )
            except Exception as exc:
                record["status"] = "error"
                record["python_error"] = str(exc)
                failures += 1
                print(f"  Error: {exc}", flush=True)

            results.append(record)

        Path(args.output).write_text(
            json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(
            f"\nCompleted {total} cohorts. Mismatches: {mismatches}, Failures: {failures}, No reference: {no_reference}. "
            f"Report written to {args.output}",
            flush=True,
        )
        return 0 if (mismatches == 0 and failures == 0) else 1
    finally:
        try:
            if hasattr(con, "close"):
                con.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
