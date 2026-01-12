from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from mitos.testing.circe_json_walk import iter_circe_inventory_fields_present


@dataclass(frozen=True)
class SweepRow:
    phenotype: str
    json_path: Path
    python_rows: int | None
    circe_rows: int | None


def load_sweep_report(path: Path) -> list[SweepRow]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: list[SweepRow] = []
    for entry in payload:
        out.append(
            SweepRow(
                phenotype=str(entry.get("phenotype")),
                json_path=Path(entry["json_path"]),
                python_rows=int(entry["python_rows"]) if entry.get("python_rows") is not None else None,
                circe_rows=int(entry["circe_rows"]) if entry.get("circe_rows") is not None else None,
            )
        )
    return out


def _iter_criteria_payloads(obj: Any) -> Iterable[tuple[str, dict[str, Any]]]:
    """
    Yield (CriteriaType, payload) pairs from a Circe cohort JSON dict.
    This stays on the raw JSON to preserve `@JsonProperty` key names.
    """

    def walk(node: Any) -> Iterable[tuple[str, dict[str, Any]]]:
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, dict) and key[:1].isupper():
                    yield key, value
                yield from walk(value)
        elif isinstance(node, list):
            for item in node:
                yield from walk(item)

    yield from walk(obj)


def build_field_usage_report(
    *,
    sweep_rows: list[SweepRow],
    circe_inventory: dict[str, list[dict[str, str]]],
) -> dict[str, dict[str, Any]]:
    """
    Compute field usage for each (CriteriaType, @JsonProperty) as:
      - used_in: phenotypes that mention the field
      - nonzero_in_{circe,python,both}: among those, phenotypes producing >0 rows
      - zero_in_both: among those, phenotypes producing 0 rows in both engines
    """
    report: dict[str, dict[str, Any]] = {}

    for criteria_type, fields in circe_inventory.items():
        for f in fields:
            prop = f["json_property"]
            key = f"{criteria_type}.{prop}"
            report[key] = {
                "criteria_type": criteria_type,
                "json_property": prop,
                "used_in": 0,
                "nonzero_in_circe": 0,
                "nonzero_in_python": 0,
                "nonzero_in_both": 0,
                "zero_in_both": 0,
                "examples": {"used": [], "nonzero_in_both": [], "zero_in_both": []},
            }

    for row in sweep_rows:
        cohort_json = json.loads(row.json_path.read_text(encoding="utf-8"))
        used = set(
            iter_circe_inventory_fields_present(
                cohort_json,
                circe_inventory=circe_inventory,
                root_class="CohortExpression",
            )
        )

        python_rows = int(row.python_rows or 0)
        circe_rows = int(row.circe_rows or 0)
        both_nonzero = python_rows > 0 and circe_rows > 0
        both_zero = python_rows == 0 and circe_rows == 0

        for key in used:
            entry = report[key]
            entry["used_in"] += 1
            if circe_rows > 0:
                entry["nonzero_in_circe"] += 1
            if python_rows > 0:
                entry["nonzero_in_python"] += 1
            if both_nonzero:
                entry["nonzero_in_both"] += 1
            if both_zero:
                entry["zero_in_both"] += 1

            examples = entry["examples"]
            if len(examples["used"]) < 5:
                examples["used"].append(row.phenotype)
            if both_nonzero and len(examples["nonzero_in_both"]) < 5:
                examples["nonzero_in_both"].append(row.phenotype)
            if both_zero and len(examples["zero_in_both"]) < 5:
                examples["zero_in_both"].append(row.phenotype)

    return report
