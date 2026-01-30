from __future__ import annotations

import json

from mitos.testing.circe_unknown_fields import iter_unknown_circe_fields


def test_unknown_fields_detected_against_inventory():
    inventory = json.loads(
        open(
            "tests/scenarios/fieldcases/circe_field_inventory.json",
            "r",
            encoding="utf-8",
        ).read()
    )

    cohort_json = {
        "Title": "t",
        "ConceptSets": [{"id": 1, "name": "cs", "expression": {}}],
        "PrimaryCriteria": {
            "CriteriaList": [
                {
                    "ConditionOccurrence": {
                        "CodesetId": 1,
                        # Not present in CirceR 1.11.3 inventory (but exists in circe-be source):
                        "ConditionStatusCS": {"CodesetId": 2, "IsExclusion": False},
                    }
                }
            ]
        },
    }

    unknown = list(
        iter_unknown_circe_fields(
            cohort_json,
            circe_inventory=inventory,
            root_class="CohortExpression",
        )
    )
    keys = {u.key for u in unknown}
    assert "ConditionOccurrence.ConditionStatusCS" in keys


def test_cdm_version_range_is_unknown_in_circe_inventory():
    inventory = json.loads(
        open(
            "tests/scenarios/fieldcases/circe_field_inventory.json",
            "r",
            encoding="utf-8",
        ).read()
    )
    cohort_json = {"Title": "t", "cdmVersionRange": "5.3"}
    unknown = list(
        iter_unknown_circe_fields(
            cohort_json,
            circe_inventory=inventory,
            root_class="CohortExpression",
        )
    )
    keys = {u.key for u in unknown}
    assert "CohortExpression.cdmVersionRange" in keys
