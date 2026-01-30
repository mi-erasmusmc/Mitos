from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest

from mitos.testing.circe_oracle import CirceSqlConfig, generate_circe_sql_batch_via_r
from mitos.testing.fieldcases.harness import (
    assert_same_rows,
    require_non_empty,
    rscript_available,
    run_fieldcase,
)
from tests.scenarios.fieldcases.cases import ALL


@pytest.fixture(scope="session")
def circe_sql_by_case_name() -> dict[str, str]:
    """
    Pre-generate all Circe SQL in one R invocation to keep FieldCases fast.
    """
    if not rscript_available():
        pytest.skip("Rscript not available; FieldCases require CirceR + SqlRender.")

    tmp_dir = Path(tempfile.mkdtemp(prefix="mitos_fieldcases_json_"))
    try:
        cfgs: dict[str, CirceSqlConfig] = {}
        for case in ALL:
            json_path = tmp_dir / f"{case.name}.json"
            json_path.write_text(
                json.dumps(case.cohort_json, indent=2) + "\n", encoding="utf-8"
            )
            cfgs[case.name] = CirceSqlConfig(
                json_path=json_path,
                cdm_schema="main",
                vocab_schema="main",
                result_schema="main",
                target_schema="main",
                target_table="_circe_cohort_rows",
                cohort_id=case.cohort_id,
                target_dialect="duckdb",
            )
        sql_by_name, _ = generate_circe_sql_batch_via_r(cfgs)
        return sql_by_name
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.mark.parametrize("case", ALL, ids=lambda c: c.name)
def test_fieldcase_matches_circe(case, circe_sql_by_case_name: dict[str, str]):
    if not rscript_available():
        pytest.skip("Rscript not available; FieldCases require CirceR + SqlRender.")

    circe_rows, python_rows = run_fieldcase(
        case, circe_sql=circe_sql_by_case_name[case.name]
    )
    require_non_empty(circe_rows, case_name=case.name, engine="Circe")
    assert_same_rows(circe_rows, python_rows)
