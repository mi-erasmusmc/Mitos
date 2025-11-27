from datetime import datetime

import ibis
import polars as pl

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import Measurement

import ibis_cohort.builders.measurement  # noqa: F401 ensure registration


def make_context(conn, codes):
    codeset_expr = ibis.memtable([{"codeset_id": 1, "concept_id": code} for code in codes])
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_measurement_unit_normalization_handles_pounds():
    conn = ibis.duckdb.connect(database=":memory:")
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1, 2],
            "person_id": [1, 2],
            "measurement_concept_id": [100, 100],
            "measurement_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "value_as_number": [95.0, 200.0],
            "unit_concept_id": [9529, 3195625],  # kilogram, pound
        }
    )
    conn.create_table("measurement", measurement_df, overwrite=True)

    ctx = make_context(conn, [100])
    criteria = Measurement(
        **{
            "CodesetId": 1,
            "Unit": [
                {"CONCEPT_ID": 9529},
                {"CONCEPT_ID": 3195625},
            ],
            "ValueAsNumber": {"Value": 90, "Op": "bt", "Extent": 110},
        }
    )

    events = build_events(criteria, ctx)
    df = events.to_polars()

    assert set(df["person_id"].to_list()) == {1, 2}, "pound value should be normalized to kilograms"


def test_measurement_unit_normalization_handles_cell_counts():
    conn = ibis.duckdb.connect(database=":memory:")
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1, 2],
            "person_id": [1, 2],
            "measurement_concept_id": [200, 200],
            "measurement_date": [datetime(2020, 2, 1), datetime(2020, 2, 2)],
            "value_as_number": [1.0, 1000.0],
            "unit_concept_id": [9444, 8784],  # 10^9/L, cells/uL
        }
    )
    conn.create_table("measurement", measurement_df, overwrite=True)

    ctx = make_context(conn, [200])
    criteria = Measurement(
        **{
            "CodesetId": 1,
            "Unit": [
                {"CONCEPT_ID": 9444},
                {"CONCEPT_ID": 8784},
            ],
            "ValueAsNumber": {"Value": 0.5, "Op": "bt", "Extent": 1.5},
        }
    )

    events = build_events(criteria, ctx)
    df = events.to_polars()

    assert set(df["person_id"].to_list()) == {1, 2}, "cells/uL values should convert to 10^9/L scale"
