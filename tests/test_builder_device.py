from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import DeviceExposure

import ibis_cohort.builders.device_exposure  # noqa: F401


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [501]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_device_exposure_builder_filters_codeset():
    conn = ibis.duckdb.connect(database=":memory:")
    device_df = pl.DataFrame(
        {
            "device_exposure_id": [1, 2],
            "person_id": [1, 1],
            "device_concept_id": [501, 502],
            "device_exposure_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "device_exposure_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
            "device_type_concept_id": [0, 0],
            "visit_occurrence_id": [1, 1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("device_exposure", device_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = DeviceExposure(**{"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
