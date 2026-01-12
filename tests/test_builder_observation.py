from datetime import datetime
import uuid

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import Observation


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1, 2], "concept_id": [301, 10]})
    name = f"codesets_{uuid.uuid4().hex}"
    conn.create_table(name, codeset_expr, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


def test_observation_builder_filters_codeset_and_first():
    conn = ibis.duckdb.connect(database=":memory:")
    observation_df = pl.DataFrame(
        {
            "observation_id": [1, 2],
            "person_id": [1, 1],
            "observation_concept_id": [301, 301],
            "observation_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "value_as_number": [1.0, 2.0],
            "value_as_concept_id": [10, 10],
            "unit_concept_id": [0, 0],
            "observation_type_concept_id": [0, 0],
            "visit_occurrence_id": [1, 1],
        }
    )
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("observation", observation_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = Observation(**{"CodesetId": 1, "First": True})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]


def test_observation_filters_source_concept():
    conn = ibis.duckdb.connect(database=":memory:")
    observation_df = pl.DataFrame(
        {
            "observation_id": [1, 2],
            "person_id": [1, 2],
            "observation_concept_id": [301, 301],
            "observation_source_concept_id": [10, 20],
            "observation_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "value_as_number": pl.Series([None, None], dtype=pl.Float64),
            "value_as_concept_id": pl.Series([None, None], dtype=pl.Int64),
            "unit_concept_id": pl.Series([None, None], dtype=pl.Int64),
            "observation_type_concept_id": [0, 0],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    conn.create_table("observation", observation_df, overwrite=True)

    ctx = make_context(conn)
    criteria = Observation(**{"CodesetId": 1, "ObservationSourceConcept": 2})

    events = build_events(criteria, ctx)
    df = events.to_polars()

    assert df.shape[0] == 1
    assert df["person_id"][0] == 1
