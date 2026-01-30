from datetime import datetime
import uuid

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import DeviceExposure


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1, 2], "concept_id": [501, 10]})
    name = f"codesets_{uuid.uuid4().hex}"
    conn.create_table(name, codeset_expr, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


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
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("device_exposure", device_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = DeviceExposure.model_validate({"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]


def test_device_exposure_filters_source_concept():
    conn = ibis.duckdb.connect(database=":memory:")
    device_df = pl.DataFrame(
        {
            "device_exposure_id": [1, 2],
            "person_id": [1, 2],
            "device_concept_id": [501, 501],
            "device_source_concept_id": [10, 20],
            "device_exposure_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "device_exposure_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 3)],
            "device_type_concept_id": [0, 0],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    conn.create_table("device_exposure", device_df, overwrite=True)

    ctx = make_context(conn)
    criteria = DeviceExposure.model_validate({"CodesetId": 1, "DeviceSourceConcept": 2})

    events = build_events(criteria, ctx)
    df = events.to_polars()

    assert df.shape[0] == 1
    assert df["person_id"][0] == 1
