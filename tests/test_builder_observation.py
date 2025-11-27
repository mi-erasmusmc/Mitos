from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import Observation

import ibis_cohort.builders.observation  # noqa: F401


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [301]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


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
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("observation", observation_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = Observation(**{"CodesetId": 1, "First": True})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
