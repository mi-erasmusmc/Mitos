from __future__ import annotations

import argparse
import json
from pathlib import Path

from mitos.testing.field_usage import build_field_usage_report, load_sweep_report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute Circe-field usage stats across a sweep report.")
    parser.add_argument(
        "--sweep",
        type=Path,
        default=Path("report_sweep_new.json"),
        help="Path to the sweep report JSON (output of scripts/sweep_phenotypes.py).",
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("tests/scenarios/fieldcases/circe_field_inventory.json"),
        help="Path to Circe field inventory JSON.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("reports/field_usage.json"),
        help="Where to write the usage report JSON.",
    )
    args = parser.parse_args(argv)

    sweep_rows = load_sweep_report(args.sweep)
    circe_inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    report = build_field_usage_report(sweep_rows=sweep_rows, circe_inventory=circe_inventory)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

