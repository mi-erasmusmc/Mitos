from datetime import datetime
import uuid

import ibis
import polars as pl

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import Measurement

def make_context(conn, codes, extra_codesets=None):
    rows = [{"codeset_id": 1, "concept_id": code} for code in codes]
    if extra_codesets:
        rows.extend(extra_codesets)
    codeset_expr = ibis.memtable(rows)

    # Materialize memtable to a real DuckDB temp table to allow raw SQL compilation
    name = f"codesets_{uuid.uuid4().hex}"
    conn.create_table(name, codeset_expr, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


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

    assert set(df["person_id"].to_list()) == {1, 2}, (
        "pound value should be normalized to kilograms"
    )


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

    assert set(df["person_id"].to_list()) == {1, 2}, (
        "cells/uL values should convert to 10^9/L scale"
    )


def test_measurement_filters_source_concept():
    conn = ibis.duckdb.connect(database=":memory:")
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1, 2],
            "person_id": [1, 2],
            "measurement_concept_id": [300, 300],
            "measurement_source_concept_id": [10, 20],
            "measurement_date": [datetime(2021, 1, 1), datetime(2021, 1, 2)],
        }
    )
    conn.create_table("measurement", measurement_df, overwrite=True)

    ctx = make_context(conn, [300], extra_codesets=[{"codeset_id": 2, "concept_id": 10}])
    criteria = Measurement(
        **{
            "CodesetId": 1,
            "MeasurementSourceConcept": 2,
        }
    )

    events = build_events(criteria, ctx)
    df = events.to_polars()

    assert df.shape[0] == 1
    assert df["person_id"][0] == 1
