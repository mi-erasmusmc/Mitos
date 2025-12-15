from __future__ import annotations

import ibis

from mitos.builders.pipeline import _assign_primary_event_ids


def test_assign_primary_event_ids_avoids_global_row_number():
    # A minimal table shape for _assign_primary_event_ids (no backend execution).
    t = ibis.table(
        {
            "person_id": "int64",
            "event_id": "int64",
            "start_date": "date",
            "end_date": "date",
            "visit_occurrence_id": "int64",
        },
        name="events",
    )
    out = _assign_primary_event_ids(t)

    con = ibis.duckdb.connect()
    sql = con.compile(out)
    # We should not generate a global ROW_NUMBER() OVER (ORDER BY ...) which forces
    # a single-partition/global sort on Spark/Databricks.
    assert "row_number() over (order by" not in sql.lower()
    # We expect a partitioned row_number by person_id.
    assert "partition by" in sql.lower()

