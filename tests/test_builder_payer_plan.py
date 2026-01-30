from datetime import datetime
import uuid

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import PayerPlanPeriod


def make_context(conn):
    codesets = ibis.memtable(
        {
            "codeset_id": [20, 21, 22, 23, 24, 25, 26, 27],
            "concept_id": [901, 1001, 1101, 1201, 1301, 1401, 1501, 1601],
        }
    )
    name = f"codeset_{uuid.uuid4().hex}"
    conn.create_table(name, codesets, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


def test_payer_plan_period_filters_concepts_and_user_period():
    conn = ibis.duckdb.connect(database=":memory:")

    payer_plan_df = pl.DataFrame(
        {
            "payer_plan_period_id": [1, 2],
            "person_id": [1, 1],
            "payer_plan_period_start_date": [
                datetime(2020, 1, 1),
                datetime(2020, 6, 1),
            ],
            "payer_plan_period_end_date": [datetime(2020, 4, 1), datetime(2020, 7, 1)],
            "payer_concept_id": [901, 999],
            "plan_concept_id": [1001, 999],
            "sponsor_concept_id": [1101, 1101],
            "stop_reason_concept_id": [1201, 1201],
            "payer_source_concept_id": [1301, 1301],
            "plan_source_concept_id": [1401, 1401],
            "sponsor_source_concept_id": [1501, 1501],
            "stop_reason_source_concept_id": [1601, 1601],
        }
    )
    conn.create_table("payer_plan_period", payer_plan_df, overwrite=True)

    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = PayerPlanPeriod(
        **{
            "First": True,
            "PeriodLength": {"Value": 60, "Op": "gte"},
            "AgeAtStart": {"Value": 30, "Op": "gte"},
            "AgeAtEnd": {"Value": 30, "Op": "gte"},
            "Gender": [{"CONCEPT_ID": 8507}],
            "PayerConcept": 20,
            "PlanConcept": 21,
            "SponsorConcept": 22,
            "StopReasonConcept": 23,
            "PayerSourceConcept": 24,
            "PlanSourceConcept": 25,
            "SponsorSourceConcept": 26,
            "StopReasonSourceConcept": 27,
            "UserDefinedPeriod": {"StartDate": "2020-01-05", "EndDate": "2020-03-01"},
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
    assert result["start_date"][0].date().isoformat() == "2020-01-05"
    assert result["end_date"][0].date().isoformat() == "2020-03-01"
