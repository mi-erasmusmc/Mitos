from __future__ import annotations

import json
from pathlib import Path

import pytest

from mitos.testing.circe_json_walk import iter_circe_inventory_fields_present
from mitos.testing.field_usage import load_sweep_report
from mitos.testing.fieldcases.harness import rscript_available
from tests.scenarios.fieldcases.cases import ALL


def _load_circe_inventory() -> dict[str, list[dict[str, str]]]:
    path = Path("tests/scenarios/fieldcases/circe_field_inventory.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload


def _normalize_sweep_json_path(raw: str) -> Path:
    path = Path(raw)
    if path.exists():
        return path

    marker = "fixtures/phenotypes"
    parts = raw.replace("\\", "/").split(marker)
    if len(parts) == 2 and parts[1].startswith("/"):
        candidate = Path(marker + parts[1])
        if candidate.exists():
            return candidate

    raise FileNotFoundError(f"Cannot resolve sweep json_path: {raw}")


def _collect_sweep_used_keys(
    *, inventory: dict[str, list[dict[str, str]]]
) -> tuple[set[str], set[str]]:
    sweep = load_sweep_report(Path("report_sweep_new.json"))
    used: set[str] = set()
    used_by_field: dict[str, int] = {}
    nonzero_in_both_by_field: dict[str, int] = {}

    for row in sweep:
        json_path = _normalize_sweep_json_path(str(row.json_path))
        cohort_json = json.loads(json_path.read_text(encoding="utf-8"))
        present = set(
            iter_circe_inventory_fields_present(
                cohort_json,
                circe_inventory=inventory,
                root_class="CohortExpression",
            )
        )
        used |= present

        python_rows = int(row.python_rows or 0)
        circe_rows = int(row.circe_rows or 0)
        both_nonzero = python_rows > 0 and circe_rows > 0
        for key in present:
            used_by_field[key] = used_by_field.get(key, 0) + 1
            if both_nonzero:
                nonzero_in_both_by_field[key] = nonzero_in_both_by_field.get(key, 0) + 1

    never_validated = {
        key
        for key, count in used_by_field.items()
        if count and nonzero_in_both_by_field.get(key, 0) == 0
    }
    return used, never_validated


def _collect_covered_keys_from_fieldcases(
    *, inventory: dict[str, list[dict[str, str]]]
) -> dict[str, int]:
    covered_counts: dict[str, int] = {}
    for case in ALL:
        present = set(
            iter_circe_inventory_fields_present(
                case.cohort_json,
                circe_inventory=inventory,
                root_class="CohortExpression",
            )
        )
        for key in present:
            covered_counts[key] = covered_counts.get(key, 0) + 1
    return covered_counts


def test_fieldcases_cover_all_fields_used_by_fixtures():
    if not rscript_available():
        pytest.skip("Rscript not available; FieldCase coverage gate is CI-only.")

    if not Path("report_sweep_new.json").exists():
        pytest.skip("report_sweep_new.json not available (local-only sweep artifact).")

    inventory = _load_circe_inventory()
    used, never_validated = _collect_sweep_used_keys(inventory=inventory)
    covered_counts = _collect_covered_keys_from_fieldcases(inventory=inventory)

    missing = sorted(key for key in used if covered_counts.get(key, 0) == 0)
    assert not missing, f"Missing FieldCases for: {missing}"

    missing_two_case = sorted(key for key in used if covered_counts.get(key, 0) < 2)
    assert not missing_two_case, (
        f"Need ≥2 FieldCases for sweep-used fields: {missing_two_case}"
    )
