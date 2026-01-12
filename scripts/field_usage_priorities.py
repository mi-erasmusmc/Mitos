from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _priority_key(entry: dict[str, Any]) -> tuple[int, int, float, str]:
    used_in = int(entry.get("used_in", 0))
    nonzero_in_both = int(entry.get("nonzero_in_both", 0))
    zero_in_both = int(entry.get("zero_in_both", 0))
    coverage_ratio = (nonzero_in_both / used_in) if used_in else 0.0

    # Highest priority: appears in phenotypes, but never in any non-zero cohort output.
    never_validated = 1 if (used_in > 0 and nonzero_in_both == 0) else 0

    # Then: low empirical coverage ratio, high usage.
    return (
        never_validated,
        used_in,
        -coverage_ratio,
        str(entry.get("json_property", "")),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rank Circe fields by risk based on sweep usage.")
    parser.add_argument(
        "--usage",
        type=Path,
        default=Path("reports/field_usage.json"),
        help="Path to `reports/field_usage.json`.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=Path("reports/field_usage_priorities.json"),
        help="Where to write the prioritized field list (JSON).",
    )
    parser.add_argument(
        "--out-md",
        type=Path,
        default=Path("reports/field_usage_priorities.md"),
        help="Where to write the prioritized field list (Markdown).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=75,
        help="How many fields to include in the priority list.",
    )
    args = parser.parse_args(argv)

    usage = json.loads(args.usage.read_text(encoding="utf-8"))
    entries = []
    for key, value in usage.items():
        value = dict(value)
        value["key"] = key
        entries.append(value)

    # Only consider fields that actually appear in the cohort JSONs.
    used_entries = [e for e in entries if int(e.get("used_in", 0)) > 0]
    used_entries.sort(key=_priority_key, reverse=True)
    top = used_entries[: max(0, int(args.limit))]

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(top, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines: list[str] = []
    lines.append("# Field Usage Priorities")
    lines.append("")
    lines.append("Ranked from highest → lowest risk based on sweep usage.")
    lines.append("")
    lines.append("| Field | used_in | nonzero_in_both | zero_in_both | coverage | examples (zero_in_both) |")
    lines.append("|---|---:|---:|---:|---:|---|")
    for e in top:
        used_in = int(e.get("used_in", 0))
        nonzero_in_both = int(e.get("nonzero_in_both", 0))
        zero_in_both = int(e.get("zero_in_both", 0))
        coverage = (nonzero_in_both / used_in) if used_in else 0.0
        examples = ", ".join((e.get("examples", {}) or {}).get("zero_in_both", [])[:5])
        field = str(e.get("key"))
        lines.append(f"| `{field}` | {used_in} | {nonzero_in_both} | {zero_in_both} | {coverage:.3f} | {examples} |")
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

