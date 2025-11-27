from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import ObservationPeriod

import ibis_cohort.builders.observation_period  # noqa: F401


def make_context(conn):
    empty_codeset = ibis.memtable({"codeset_id": [], "concept_id": []}, schema={"codeset_id": "int64", "concept_id": "int64"})
    return BuildContext(conn, CohortBuildOptions(), empty_codeset)


def test_observation_period_user_defined_dates():
    conn = ibis.duckdb.connect(database=":memory:")

    observation_period_df = pl.DataFrame(
        {
            "observation_period_id": [1, 2],
            "person_id": [1, 1],
            "observation_period_start_date": [datetime(2020, 1, 1), datetime(2020, 5, 1)],
            "observation_period_end_date": [datetime(2020, 3, 31), datetime(2020, 7, 1)],
            "period_type_concept_id": [44814724, 44814724],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = ObservationPeriod(
        **{
            "PeriodLength": {"Value": 20, "Op": "gte"},
            "UserDefinedPeriod": {"StartDate": "2020-01-15", "EndDate": "2020-02-15"},
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert len(result) == 1
    assert result["start_date"][0].date().isoformat() == "2020-01-15"
    assert result["end_date"][0].date().isoformat() == "2020-02-15"
