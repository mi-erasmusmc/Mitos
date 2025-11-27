from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions, compile_codesets
from ibis_cohort.builders.pipeline import build_primary_events
from ibis_cohort.cohort_expression import CohortExpression


def test_build_primary_events_produces_rows():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [4182210], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    concept_ancestor_df = pl.DataFrame(
        {
            "ancestor_concept_id": pl.Series([], dtype=pl.Int64),
            "descendant_concept_id": pl.Series([], dtype=pl.Int64),
        }
    )
    conn.create_table("concept_ancestor", concept_ancestor_df, overwrite=True)
    concept_relationship_df = pl.DataFrame(
        {
            "concept_id_1": pl.Series([], dtype=pl.Int64),
            "concept_id_2": pl.Series([], dtype=pl.Int64),
            "relationship_id": pl.Series([], dtype=pl.String),
            "invalid_reason": pl.Series([], dtype=pl.String),
        }
    )
    conn.create_table("concept_relationship", concept_relationship_df, overwrite=True)

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1],
            "person_id": [1],
            "condition_concept_id": [4182210],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
            "visit_occurrence_id": [1],
        }
    )
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    observation_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {
                    "id": 1,
                    "name": "test",
                    "expression": {
                        "items": [
                            {
                                "concept": {
                                    "CONCEPT_ID": 4182210,
                                }
                            }
                        ]
                    },
                }
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {"ConditionOccurrence": {"CodesetId": 1, "First": True}}
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "First"},
            },
            "QualifiedLimit": {"Type": "First"},
            "ExpressionLimit": {"Type": "First"},
            "InclusionRules": [],
            "CensoringCriteria": [],
        }
    )

    expression.collapse_settings = None

    expression.collapse_settings = None

    options = CohortBuildOptions()
    assert expression.collapse_settings is None
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert not result.is_empty()


def test_primary_events_retains_duplicates_and_assigns_ordinals():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [4182210], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {
                "ancestor_concept_id": pl.Series([], dtype=pl.Int64),
                "descendant_concept_id": pl.Series([], dtype=pl.Int64),
            }
        ),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": pl.Series([], dtype=pl.Int64),
                "concept_id_2": pl.Series([], dtype=pl.Int64),
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1],
            "person_id": [1],
            "condition_concept_id": [4182210],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
            "visit_occurrence_id": [1],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1, 1],
            "observation_period_start_date": [datetime(2019, 1, 1), datetime(2019, 6, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31), datetime(2020, 12, 31)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {
                    "id": 1,
                    "name": "test",
                    "expression": {"items": [{"concept": {"CONCEPT_ID": 4182210}}]},
                }
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "QualifiedLimit": {"Type": "All"},
            "ExpressionLimit": {"Type": "All"},
        }
    )

    expression.collapse_settings = None
    assert expression.collapse_settings is None

    expression.collapse_settings = None

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    result = events.to_polars().sort("event_id")
    assert len(result) == 2, "Both observation periods should yield events"
    assert result["event_id"].to_list() == [1, 2], "Event ordinals must be assigned per person"


def test_primary_events_preserves_source_ids_through_collapse():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}), overwrite=True)
    conn.create_table("concept_relationship", pl.DataFrame({"concept_id_1": pl.Series([], dtype=pl.Int64), "concept_id_2": pl.Series([], dtype=pl.Int64), "relationship_id": pl.Series([], dtype=pl.String), "invalid_reason": pl.Series([], dtype=pl.String)}), overwrite=True)

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [10, 11],
            "person_id": [1, 1],
            "condition_concept_id": [1, 1],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 5)],
            "condition_end_date": [datetime(2020, 1, 3), datetime(2020, 1, 7)],
            "visit_occurrence_id": [1, 2],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
            {
                "ConceptSets": [
                    {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                ],
                "PrimaryCriteria": {
                    "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                    "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                    "PrimaryCriteriaLimit": {"Type": "All"},
                },
                "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 0}},
                "ExpressionLimit": {"Type": "All"},
                "CollapseSettings": {"CollapseType": "ERA", "EraPad": 10},
            }
        )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    df = events.to_polars()
    assert len(df) == 1
    assert df["start_date"][0] == datetime(2020, 1, 1)
    assert df["end_date"][0] == datetime(2020, 1, 7)
    assert "_source_event_id" not in df.columns, "Internal source ids should be dropped after collapse"


def test_default_end_strategy_extends_to_observation_period():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": pl.Series([], dtype=pl.Int64),
                "concept_id_2": pl.Series([], dtype=pl.Int64),
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [10],
            "person_id": [1],
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 3)],
            "visit_occurrence_id": [1],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    df = events.to_polars()
    assert len(df) == 1
    assert df["end_date"][0] == datetime(2020, 12, 31)


def test_qualified_limit_is_ignored_to_match_circe():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": pl.Series([], dtype=pl.Int64),
                "concept_id_2": pl.Series([], dtype=pl.Int64),
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [1, 1],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "QualifiedLimit": {"Type": "First"},
        }
    )
    expression.collapse_settings = None

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    df = events.to_polars()
    assert len(df) == 2
    assert set(df["start_date"].to_list()) == {datetime(2020, 1, 1), datetime(2020, 2, 1)}
