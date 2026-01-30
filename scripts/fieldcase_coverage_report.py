from __future__ import annotations

import argparse
import json
from pathlib import Path

from mitos.testing.fieldcase_coverage import (
    build_fieldcase_coverage,
    fieldcase_coverage_markdown,
    fieldcase_coverage_to_jsonable,
)

import importlib.util


def _load_fieldcases(module_path: Path):
    spec = importlib.util.spec_from_file_location("mitos_fieldcases", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load FieldCases module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        return list(module.ALL)
    except AttributeError as e:
        raise RuntimeError(f"{module_path} must define `ALL`") from e


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute coverage of Circe inventory fields by FieldCases."
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("tests/scenarios/fieldcases/circe_field_inventory.json"),
        help="Path to Circe field inventory JSON.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("reports/fieldcase_coverage.json"),
        help="Where to write the coverage JSON.",
    )
    parser.add_argument(
        "--out-md",
        type=Path,
        default=Path("reports/fieldcase_coverage.md"),
        help="Where to write the coverage summary Markdown.",
    )
    parser.add_argument(
        "--fieldcases",
        type=Path,
        default=Path("tests/scenarios/fieldcases/cases.py"),
        help="Path to the FieldCases registry module (must define `ALL`).",
    )
    args = parser.parse_args(argv)

    circe_inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    all_cases = _load_fieldcases(args.fieldcases)
    fieldcases = [(case.name, case.cohort_json) for case in all_cases]
    coverage = build_fieldcase_coverage(
        fieldcases=fieldcases, circe_inventory=circe_inventory
    )

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(
        json.dumps(fieldcase_coverage_to_jsonable(coverage), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text(
        fieldcase_coverage_markdown(coverage),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
