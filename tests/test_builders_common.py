import ibis
import polars as pl
from datetime import date, datetime

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.common import (
    apply_age_filter,
    apply_codeset_filter,
    apply_observation_window,
)
from ibis_cohort.criteria import NumericRange


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [101]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_apply_codeset_filter():
    conn = ibis.duckdb.connect(database=":memory:")
    table = ibis.memtable(
        {"person_id": [1, 2], "condition_concept_id": [101, 102]}
    )
    ctx = make_context(conn)

    filtered = apply_codeset_filter(table, "condition_concept_id", 1, ctx)
    result = filtered.to_polars()
    assert result["person_id"].to_list() == [1]


def test_apply_age_filter():
    conn = ibis.duckdb.connect(database=":memory:")
    person_df = pl.DataFrame({"person_id": [1, 2], "year_of_birth": [1980, 2000]})
    conn.create_table("person", person_df, overwrite=True)
    table = ibis.memtable(
        {"person_id": [1, 2], "condition_start_date": [date(2020, 1, 1), date(2020, 1, 1)]},
        schema=ibis.schema({"person_id": "int64", "condition_start_date": "date"}),
    )
    ctx = make_context(conn)
    age_range = NumericRange(Value=30, Op="gte")

    filtered = apply_age_filter(table, age_range, ctx, "condition_start_date")
    result = filtered.to_polars()
    assert result["person_id"].to_list() == [1]


def test_apply_observation_window():
    conn = ibis.duckdb.connect(database=":memory:")
    observation_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )
    conn.create_table("observation_period", observation_df, overwrite=True)
    events = ibis.memtable(
        {
            "person_id": [1, 1],
            "event_id": [10, 11],
            "start_date": [date(2018, 1, 1), date(2020, 6, 1)],
            "end_date": [date(2018, 1, 2), date(2020, 6, 2)],
            "visit_occurrence_id": [0, 0],
        },
        schema=ibis.schema(
            {
                "person_id": "int64",
                "event_id": "int64",
                "start_date": "date",
                "end_date": "date",
                "visit_occurrence_id": "int64",
            }
        ),
    )
    ctx = make_context(conn)

    class ObservationWindow:
        prior_days = 0
        post_days = 0

    filtered = apply_observation_window(events, ObservationWindow(), ctx)
    result = filtered.to_polars()
    assert result["event_id"].to_list() == [11]
