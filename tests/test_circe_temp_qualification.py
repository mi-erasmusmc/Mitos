from __future__ import annotations

from scripts.compare_cohort_counts import _rewrite_circe_temp_table_qualification


def test_rewrite_circe_temp_tables_qualified_in_temp_schema():
    sql = """
    DROP TABLE IF EXISTS abcdef12Codesets;
    CREATE TABLE abcdef12Codesets USING DELTA AS SELECT 1 AS x;
    INSERT INTO abcdef12Codesets SELECT 2 AS x;
    CREATE TABLE abcdef12qualified_events USING DELTA AS SELECT * FROM abcdef12Codesets;
    """
    out = _rewrite_circe_temp_table_qualification(
        sql,
        temp_schema="scratch.scratch_efridgei",
        backend="databricks",
    )
    assert "`scratch`.`scratch_efridgei`.`abcdef12Codesets`" in out
    assert "FROM `scratch`.`scratch_efridgei`.`abcdef12Codesets`" in out
    assert "`scratch`.`scratch_efridgei`.`abcdef12qualified_events`" in out
