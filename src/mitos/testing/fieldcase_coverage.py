from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from mitos.testing.circe_json_walk import iter_circe_inventory_fields_present


@dataclass(frozen=True)
class FieldCaseCoverageRow:
    key: str
    criteria_type: str
    json_property: str
    covered_by_cases: list[str]


def build_fieldcase_coverage(
    *,
    fieldcases: Iterable[tuple[str, dict[str, Any]]],
    circe_inventory: dict[str, list[dict[str, str]]],
) -> dict[str, FieldCaseCoverageRow]:
    """
    For each Circe (CriteriaType, @JsonProperty), list which FieldCases mention it.

    This is meant to answer: "Do we have at least one discriminator/edge case for this field?"
    """
    result: dict[str, FieldCaseCoverageRow] = {}

    for criteria_type, fields in circe_inventory.items():
        for entry in fields:
            prop = entry["json_property"]
            key = f"{criteria_type}.{prop}"
            result[key] = FieldCaseCoverageRow(
                key=key,
                criteria_type=criteria_type,
                json_property=prop,
                covered_by_cases=[],
            )

    for case_name, cohort_json in fieldcases:
        present = set(
            iter_circe_inventory_fields_present(
                cohort_json,
                circe_inventory=circe_inventory,
                root_class="CohortExpression",
            )
        )

        for key in sorted(present):
            if key in result:
                result[key].covered_by_cases.append(case_name)

    return result


def fieldcase_coverage_to_jsonable(
    coverage: dict[str, FieldCaseCoverageRow],
) -> dict[str, dict[str, Any]]:
    return {
        key: {
            "criteria_type": row.criteria_type,
            "json_property": row.json_property,
            "covered_by_cases": row.covered_by_cases,
        }
        for key, row in coverage.items()
    }


def fieldcase_coverage_markdown(
    coverage: dict[str, FieldCaseCoverageRow],
    *,
    show_missing: int = 100,
    show_low: int = 100,
) -> str:
    rows = list(coverage.values())
    missing = sorted([r for r in rows if not r.covered_by_cases], key=lambda r: r.key)
    low = sorted(
        [r for r in rows if 0 < len(r.covered_by_cases) < 2], key=lambda r: r.key
    )

    total = len(rows)
    covered = sum(1 for r in rows if r.covered_by_cases)
    pct = 0.0 if total == 0 else covered / total

    lines: list[str] = []
    lines.append("# FieldCase Coverage (Circe Inventory)\n")
    lines.append(f"- Total inventory fields: {total}\n")
    lines.append(f"- Covered by ≥1 FieldCase: {covered} ({pct:.1%})\n")
    lines.append(f"- Missing coverage: {len(missing)}\n")
    lines.append(f"- Only 1 covering case: {len(low)}\n")

    lines.append("\n## Missing coverage\n")
    lines.append("| Field | |\n|---|---|\n")
    for row in missing[:show_missing]:
        lines.append(f"| `{row.key}` | |\n")
    if len(missing) > show_missing:
        lines.append(f"\n... ({len(missing) - show_missing} more)\n")

    lines.append("\n## Low coverage (only one case)\n")
    lines.append("| Field | case |\n|---|---|\n")
    for row in low[:show_low]:
        lines.append(f"| `{row.key}` | `{row.covered_by_cases[0]}` |\n")
    if len(low) > show_low:
        lines.append(f"\n... ({len(low) - show_low} more)\n")

    return "".join(lines)


def load_circe_inventory(path: str) -> dict[str, list[dict[str, str]]]:
    import json

    return json.loads(open(path, "r", encoding="utf-8").read())
