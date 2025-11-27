from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import ConditionOccurrence, ConditionEra

# ensure builders are registered
import ibis_cohort.builders.condition_occurrence  # noqa: F401
import ibis_cohort.builders.condition_era  # noqa: F401
import ibis_cohort.builders.measurement  # noqa: F401


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [101]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_condition_occurrence_builder_respects_codeset_and_first():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
            "condition_source_concept_id": [1001, 1002],
            "visit_occurrence_id": [1, 2],
        }
    )
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = ConditionOccurrence(**{"CodesetId": 1, "First": True})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["person_id"].to_list() == [1]
    assert result["event_id"].to_list() == [1]


def test_condition_occurrence_builder_filters_condition_source_concepts_by_codeset_id():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 2],
            "condition_concept_id": [101, 102],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 3)],
            "condition_source_concept_id": [44827910, 999999],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)

    codesets = ibis.memtable({"codeset_id": [4], "concept_id": [44827910]})
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)
    criteria = ConditionOccurrence(**{"ConditionSourceConcept": 4})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result.shape[0] == 1
    assert result["person_id"].to_list() == [1]


def test_condition_builder_applies_nested_correlated_criteria():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
            "condition_source_concept_id": [1001, 1002],
            "visit_occurrence_id": [1, 2],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [201],
            "measurement_date": [datetime(2020, 1, 1)],
            "measurement_type_concept_id": [0],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "visit_occurrence_id": [1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_df, overwrite=True)

    codesets = ibis.memtable({"codeset_id": [1, 2], "concept_id": [101, 201]})
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)

    criteria = ConditionOccurrence(
        **{
            "CodesetId": 1,
            "CorrelatedCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"Measurement": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": 0},
                            "End": {"Days": 0, "Coeff": 1},
                            "UseEventEnd": False,
                        },
                    }
                ],
            },
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1], "Only events with matching measurements should remain"


def test_condition_era_builder_filters_length_and_first():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_era_df = pl.DataFrame(
        {
            "condition_era_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_era_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "condition_era_end_date": [datetime(2020, 2, 21), datetime(2020, 2, 10)],
            "condition_occurrence_count": [1, 3],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("condition_era", condition_era_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    codesets = ibis.memtable({"codeset_id": [1], "concept_id": [101]})
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)
    criteria = ConditionEra(**{"CodesetId": 1, "EraLength": {"Value": 20, "Op": "gte"}, "First": True})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]


def _codeset_table(pairs):
    return ibis.memtable([{"codeset_id": cid, "concept_id": concept} for cid, concept in pairs])


def test_correlated_restrict_visit_requires_shared_visit_ids():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
            "condition_source_concept_id": [1001, 1002],
            "visit_occurrence_id": [10, 20],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [201],
            "measurement_date": [datetime(2020, 1, 1)],
            "measurement_type_concept_id": [0],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "visit_occurrence_id": [999],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_df, overwrite=True)

    codesets = _codeset_table([(1, 101), (2, 201)])
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)

    base_correlated = {
        "Criteria": {"Measurement": {"CodesetId": 2}},
        "StartWindow": {
            "Start": {"Days": 0, "Coeff": 0},
            "End": {"Days": 0, "Coeff": 1},
            "UseEventEnd": False,
        },
        "Occurrence": {"Type": 0, "Count": 0},
    }

    unrestricted = ConditionOccurrence(
        **{"CodesetId": 1, "CorrelatedCriteria": {"Type": "ALL", "CriteriaList": [base_correlated]}}
    )
    result = build_events(unrestricted, ctx).to_polars()
    assert result["event_id"].to_list() == [2], "Measurement should eliminate the matching visit when restrictVisit is off"


def test_condition_occurrence_visit_type_filters_join_visits():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "condition_end_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "condition_source_concept_id": [1001, 1002],
            "visit_occurrence_id": [10, 11],
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [10, 11],
            "person_id": [1, 1],
            "visit_concept_id": [201, 202],
            "visit_source_concept_id": [901, 902],
            "visit_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "visit_end_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)

    codesets = _codeset_table([(1, 101), (2, 201)])
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)
    criteria = ConditionOccurrence(**{"CodesetId": 1, "VisitType": [{"CONCEPT_ID": 201}]})

    result = build_events(criteria, ctx).to_polars()
    assert result["event_id"].to_list() == [1]


def test_condition_occurrence_visit_source_concept_filter():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 2)],
            "condition_source_concept_id": [1001, 1002],
            "visit_occurrence_id": [10, 10],
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [10],
            "person_id": [1],
            "visit_concept_id": [201],
            "visit_source_concept_id": [999],
            "visit_start_date": [datetime(2020, 1, 1)],
            "visit_end_date": [datetime(2020, 1, 1)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)

    codesets = _codeset_table([(1, 101), (3, 102)])
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)
    criteria = ConditionOccurrence(**{"CodesetId": 1, "VisitSourceConcept": 999})

    result = build_events(criteria, ctx).to_polars()
    assert result["event_id"].to_list() == [1, 2]

    criteria_source = ConditionOccurrence(**{"CodesetId": 1, "ConditionSourceConcept": {"CodesetId": 3}})
    result = build_events(criteria_source, ctx).to_polars()
    assert result.height == 0


def test_condition_occurrence_condition_source_concept_codeset():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 2)],
            "condition_source_concept_id": [1001, 2002],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)

    codesets = _codeset_table([(1, 101), (2, 1001)])
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)
    criteria = ConditionOccurrence(**{"CodesetId": 1, "ConditionSourceConcept": {"CodesetId": 2}})

    result = build_events(criteria, ctx).to_polars()
    assert result["event_id"].to_list() == [1]


def test_correlated_ignore_observation_period_flag():
    conn = ibis.duckdb.connect(database=":memory:")
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1],
            "person_id": [1],
            "condition_concept_id": [101],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
            "visit_occurrence_id": [10],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [201],
            "measurement_date": [datetime(2018, 1, 1)],
            "measurement_type_concept_id": [0],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "visit_occurrence_id": [10],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_df, overwrite=True)

    codesets = _codeset_table([(1, 101), (2, 201)])
    ctx = BuildContext(conn, CohortBuildOptions(), codesets)

    correlated_block = {
        "Criteria": {"Measurement": {"CodesetId": 2}},
        "StartWindow": {
            "Start": {"Days": -900, "Coeff": 1},
            "End": {"Days": 0, "Coeff": 1},
            "UseEventEnd": False,
        },
        "Occurrence": {"Type": 0, "Count": 0},
    }

    default = ConditionOccurrence(
        **{"CodesetId": 1, "CorrelatedCriteria": {"Type": "ALL", "CriteriaList": [correlated_block]}}
    )
    assert (
        build_events(default, ctx).to_polars().shape[0] == 1
    ), "Measurements outside observation periods should be ignored by default"

    ignore = ConditionOccurrence(
        **{
            "CodesetId": 1,
            "CorrelatedCriteria": {
                "Type": "ALL",
                "CriteriaList": [{**correlated_block, "IgnoreObservationPeriod": True}],
            },
        }
    )
    assert (
        build_events(ignore, ctx).to_polars().is_empty()
    ), "Setting IgnoreObservationPeriod must consider measurements outside observation windows"
