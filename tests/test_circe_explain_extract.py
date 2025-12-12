from __future__ import annotations

from scripts.compare_cohort_counts import _extract_circe_select_for_explain


def test_extract_circe_select_prefers_qualified_events():
    sql = """
    DROP TABLE IF EXISTS abcdef12Codesets;
    CREATE TABLE abcdef12qualified_events USING DELTA AS SELECT 1 AS x;
    CREATE TABLE abcdef12final_cohort USING DELTA AS SELECT 2 AS y;
    """
    out = _extract_circe_select_for_explain(sql)
    assert out == "SELECT 1 AS x"


def test_extract_circe_select_falls_back_to_final_cohort():
    sql = "CREATE TABLE abcdef12final_cohort USING DELTA AS SELECT 2 AS y;"
    out = _extract_circe_select_for_explain(sql)
    assert out == "SELECT 2 AS y"
