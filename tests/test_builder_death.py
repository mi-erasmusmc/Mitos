from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import Death

import ibis_cohort.builders.death  # noqa: F401


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [701]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_death_builder_generates_events():
    conn = ibis.duckdb.connect(database=":memory:")
    death_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "death_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "death_type_concept_id": [0, 0],
            "cause_concept_id": [701, 702],
        }
    )
    person_df = pl.DataFrame({"person_id": [1, 2], "year_of_birth": [1980, 1970], "gender_concept_id": [8507, 8532]})
    conn.create_table("death", death_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = Death(**{"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["person_id"].to_list() == [1]
