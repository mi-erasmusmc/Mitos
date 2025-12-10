import json
from pathlib import Path

import pytest

from ibis_cohort.cohort_expression import CohortExpression


COHORT_FILES = sorted(Path("cohorts").glob("*.json")) + sorted(
    Path("fixtures/phenotypes").glob("*.json")
)
UNSUPPORTED_CRITERIA = set()


def contains_unsupported(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key in UNSUPPORTED_CRITERIA:
                return True
            if contains_unsupported(value):
                return True
    elif isinstance(node, list):
        return any(contains_unsupported(item) for item in node)
    return False


def strip_none(value):
    if isinstance(value, dict):
        return {k: strip_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [strip_none(v) for v in value]
    return value


@pytest.mark.parametrize(
    "cohort_path", COHORT_FILES, ids=[p.name for p in COHORT_FILES]
)
def test_cohort_expression_round_trip(cohort_path: Path):
    original = json.loads(cohort_path.read_text())

    if contains_unsupported(original):
        pytest.skip("Phenotype uses unsupported criteria types.")

    parsed = CohortExpression.model_validate_json(cohort_path.read_text())
    regenerated = parsed.model_dump(
        by_alias=True, exclude_none=True, exclude_defaults=False
    )

    assert strip_none(regenerated) == strip_none(original)
