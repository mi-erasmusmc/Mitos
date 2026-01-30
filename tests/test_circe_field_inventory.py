from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mitos.testing.circe_inventory import (
    circe_inventory_to_jsonable,
    extract_circe_field_inventory,
    find_circe_jar,
)
from mitos.tables import CRITERIA_TYPE_MAP, parse_single_criteria
from pydantic import AliasChoices, AliasPath


INVENTORY_PATH = Path("tests/scenarios/fieldcases/circe_field_inventory.json")


def _sample_value_for_java_type(java_type: str) -> Any:
    java_type = java_type.strip()

    if java_type in {"Integer", "int", "Long", "long"}:
        return 1
    if java_type in {"Double", "double", "Float", "float"}:
        return 1.0
    if java_type in {"Boolean", "boolean"}:
        return False
    if java_type == "String":
        return "x"

    if java_type in {"Concept[]", "ConceptSetItem[]"}:
        return []
    if java_type == "ConceptSetSelection":
        return {"CodesetId": 1, "IsExclusion": False}
    if java_type == "NumericRange":
        return {"Value": 1, "Op": "gte"}
    if java_type == "DateRange":
        return {"Value": "2001-01-01", "Op": "gte"}
    if java_type == "TextFilter":
        return {"Text": "x", "Op": "contains"}
    if java_type == "Endpoint":
        return {"Days": 0, "Coeff": 1}
    if java_type == "Window":
        return {"Start": {"Days": 0, "Coeff": 1}, "End": {"Days": 0, "Coeff": 1}}
    if java_type == "DateAdjustment":
        return {
            "StartWith": "StartDate",
            "StartOffset": 0,
            "EndWith": "EndDate",
            "EndOffset": 0,
        }
    if java_type == "Occurrence":
        return {"Type": 0, "Count": 1}
    if java_type in {"CriteriaGroup", "DemographicCriteria"}:
        return {
            "Type": "ALL",
            "Count": 1,
            "CriteriaList": [],
            "DemographicCriteriaList": [],
            "Groups": [],
        }

    if java_type.endswith("[]"):
        return []
    if java_type.startswith("List<"):
        return []

    # Many Circe Java types are complex (e.g. EndStrategy); those are handled by integration tests.
    return None


def test_circe_field_inventory_is_up_to_date():
    try:
        circe_jar = find_circe_jar()
    except Exception as e:
        pytest.skip(f"Circe JAR not available for inventory extraction: {e}")

    inventory = extract_circe_field_inventory(circe_jar)
    expected = circe_inventory_to_jsonable(inventory)

    committed = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    assert committed == expected


def _collect_validation_aliases(model_cls) -> set[str]:
    aliases: set[str] = set()
    for field_info in model_cls.model_fields.values():
        if field_info.alias:
            aliases.add(str(field_info.alias))
        validation_alias = getattr(field_info, "validation_alias", None)
        if validation_alias is None:
            continue
        if isinstance(validation_alias, str):
            aliases.add(validation_alias)
        elif isinstance(validation_alias, AliasChoices):
            for choice in validation_alias.choices:
                if isinstance(choice, str):
                    aliases.add(choice)
        elif isinstance(validation_alias, AliasPath):
            # Nested aliases aren't relevant to top-level criteria JSON keys.
            continue
    return aliases


@pytest.mark.parametrize("criteria_type", sorted(CRITERIA_TYPE_MAP.keys()))
def test_pydantic_models_accept_all_circe_criteria_properties(criteria_type: str):
    """
    Assert that for each Circe Criteria type, our Pydantic model accepts every @JsonProperty key.
    This is a direct coverage check (no execution / SQL generation).
    """
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    java_fields: list[dict[str, str]] = inventory.get(criteria_type, [])
    if not java_fields:
        pytest.skip(f"No Circe inventory entry for {criteria_type}")

    required = {entry["json_property"] for entry in java_fields}
    model_cls = CRITERIA_TYPE_MAP[criteria_type]
    accepted = _collect_validation_aliases(model_cls)

    missing = sorted(required - accepted)
    assert not missing, (
        f"{criteria_type} missing aliases for Circe properties: {missing}"
    )


@pytest.mark.parametrize("criteria_type", sorted(CRITERIA_TYPE_MAP.keys()))
def test_all_circe_json_properties_parse_for_criteria(criteria_type: str):
    """
    Ensures our Pydantic criteria models accept the full set of @JsonProperty keys
    in Circe's Java source (even if some are not yet implemented in query builders).
    """
    inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
    java_fields: list[dict[str, str]] = inventory.get(criteria_type, [])
    if not java_fields:
        pytest.skip(f"No Circe inventory entry for {criteria_type}")

    payload: dict[str, Any] = {}
    for entry in java_fields:
        prop = entry["json_property"]
        sample = _sample_value_for_java_type(entry["java_type"])
        if sample is None:
            continue
        payload[prop] = sample

    # CodesetId is the most common discriminator; add it if Circe lists it.
    if "CodesetId" in {e["json_property"] for e in java_fields}:
        payload.setdefault("CodesetId", 1)

    criteria_dict = {criteria_type: payload}
    parsed = parse_single_criteria(criteria_dict)
    assert parsed is not None
