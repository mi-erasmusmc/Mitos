from datetime import datetime

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events
from mitos.cohort_expression import CohortExpression


def test_pipeline_with_additional_inclusion_censoring_and_collapse():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [1, 1],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 10)],
            "condition_end_date": [datetime(2020, 1, 5), datetime(2020, 1, 12)],
            "visit_occurrence_id": [1, 1],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [2],
            "measurement_date": [datetime(2020, 1, 2)],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "measurement_type_concept_id": [0],
            "visit_occurrence_id": [1],
        }
    )
    observation_df = pl.DataFrame(
        {
            "observation_id": [1],
            "person_id": [1],
            "observation_concept_id": [2],
            "observation_date": [datetime(2020, 1, 4)],
            "value_as_number": [0.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "observation_type_concept_id": [0],
            "visit_occurrence_id": [1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("observation", observation_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "measurement", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {
                            "Measurement": {
                                "CodesetId": 2,
                                "OccurrenceStartDate": {"Value": "2020-01-01", "Op": "gte"},
                            }
                        },
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": 0},
                            "End": {"Days": 3, "Coeff": 1},
                            "UseEventEnd": False,
                        },
                    }
                ],
            },
            "InclusionRules": [
                {
                    "name": "measurement rule",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [
                            {
                                "Criteria": {"Measurement": {"CodesetId": 2}},
                                "StartWindow": {
                                    "Start": {"Days": 0, "Coeff": 0},
                                    "End": {"Days": 3, "Coeff": 1},
                                    "UseEventEnd": False,
                                },
                            }
                        ],
                    },
                }
            ],
            "CensoringCriteria": [{"Observation": {"CodesetId": 2}}],
            "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 1}},
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 10},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()

    # Two condition events collapse into one; end date is the earlier of end-strategy and censor date.
    assert len(result) == 1
    row = result.row(0, named=True)
    assert row["start_date"] == datetime(2020, 1, 1)
    assert row["end_date"] == datetime(2020, 1, 4)


def test_inclusion_rule_counts_distinct_domain_concepts():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [100, 200, 201, 202], "invalid_reason": ["", "", "", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 2],
            "condition_concept_id": [100, 100],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 2)],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    drug_era_df = pl.DataFrame(
        {
            "drug_era_id": [10, 11, 12, 20, 21],
            "person_id": [1, 1, 1, 2, 2],
            "drug_concept_id": [200, 201, 201, 202, 202],
            "drug_era_start_date": [
                datetime(2020, 1, 5),
                datetime(2020, 1, 15),
                datetime(2020, 1, 25),
                datetime(2020, 1, 5),
                datetime(2020, 1, 15),
            ],
            "drug_era_end_date": [
                datetime(2020, 1, 6),
                datetime(2020, 1, 16),
                datetime(2020, 1, 26),
                datetime(2020, 1, 6),
                datetime(2020, 1, 16),
            ],
            "drug_exposure_count": [1, 1, 1, 1, 1],
            "gap_days": [0, 0, 0, 0, 0],
        }
    )
    person_df = pl.DataFrame({"person_id": [1, 2], "year_of_birth": [1980, 1985], "gender_concept_id": [8507, 8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "observation_period_start_date": [datetime(2019, 1, 1), datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1), datetime(2021, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("drug_era", drug_era_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 100}}]}},
                {
                    "id": 2,
                    "name": "drug era",
                    "expression": {
                        "items": [
                            {"concept": {"CONCEPT_ID": 200}},
                            {"concept": {"CONCEPT_ID": 201}},
                            {"concept": {"CONCEPT_ID": 202}},
                        ]
                    },
                },
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "InclusionRules": [
                {
                    "name": "multi domain concept eras",
                    "expression": {
                        "Type": "ANY",
                        "CriteriaList": [
                            {
                                "Criteria": {"DrugEra": {"CodesetId": 2}},
                                "StartWindow": {
                                    "Start": {"Days": 365, "Coeff": -1},
                                    "End": {"Days": 365, "Coeff": 1},
                                    "UseEventEnd": False,
                                },
                                "Occurrence": {
                                    "Type": 2,
                                    "Count": 2,
                                    "IsDistinct": True,
                                    "CountColumn": "DOMAIN_CONCEPT",
                                },
                            }
                        ],
                    },
                }
            ],
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert result["person_id"].to_list() == [1]


def test_end_strategy_date_offset_capped_by_observation_period():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
            "visit_occurrence_id": [1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 12, 25)],
            "observation_period_end_date": [datetime(2020, 1, 5)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

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
            "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 14}},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert len(result) == 1
    assert result.row(0, named=True)["end_date"] == datetime(2020, 1, 5)


def test_collapse_uses_running_max_end_dates():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2, 3],
            "person_id": [1, 1, 1],
            "condition_concept_id": [1, 1, 1],
            "condition_start_date": [
                datetime(2020, 1, 1),
                datetime(2020, 1, 15),
                datetime(2020, 1, 20),
            ],
            "condition_end_date": [
                datetime(2020, 2, 1),
                datetime(2020, 1, 16),
                datetime(2020, 1, 25),
            ],
            "visit_occurrence_id": [1, 1, 1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

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
                "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
            }
        )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert len(result) == 1
    row = result.row(0, named=True)
    assert row["start_date"] == datetime(2020, 1, 1)
    assert row["end_date"] == datetime(2020, 2, 1)


def test_additional_criteria_window_anchor_uses_index_start():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2022, 1, 1), datetime(2020, 1, 2)],
            "visit_occurrence_id": [None, 10],
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [10],
            "person_id": [1],
            "visit_concept_id": [2],
            "visit_start_date": [datetime(2020, 1, 1)],
            "visit_end_date": [datetime(2020, 1, 5)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2023, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 0}},
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"VisitOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": -1},
                            "End": {"Days": 0, "Coeff": 1},
                            "UseEventEnd": False,
                            "UseIndexEnd": False,
                        },
                        "EndWindow": {
                            "Start": {"Days": 0, "Coeff": -1},
                            "UseEventEnd": True,
                            "UseIndexEnd": False,
                        },
                        "Occurrence": {"Type": 2, "Count": 1},
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["start_date"] == datetime(2020, 1, 1)
    assert row["end_date"] == datetime(2022, 1, 1)


def test_correlated_criteria_enforce_observation_period_end():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
            "visit_occurrence_id": pl.Series([None], dtype=pl.Int64),
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [10],
            "person_id": [1],
            "visit_concept_id": [2],
            "visit_start_date": [datetime(2020, 1, 1)],
            "visit_end_date": [datetime(2020, 3, 1)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 2, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"VisitOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": -1},
                            "End": {"Days": 0, "Coeff": 1},
                            "UseEventEnd": False,
                            "UseIndexEnd": False,
                        },
                        "EndWindow": {
                            "Start": {"Days": 0, "Coeff": -1},
                            "UseEventEnd": True,
                            "UseIndexEnd": False,
                        },
                        "Occurrence": {"Type": 2, "Count": 1},
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_visit_occurrence_provider_specialty_filters_correlated_events():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_start_date": [datetime(2020, 1, 15), datetime(2020, 2, 15)],
            "condition_end_date": [datetime(2020, 1, 16), datetime(2020, 2, 16)],
            "visit_occurrence_id": [10, 11],
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [10, 11],
            "person_id": [1, 1],
            "visit_concept_id": [100, 100],
            "visit_start_date": [datetime(2020, 1, 14), datetime(2020, 2, 14)],
            "visit_end_date": [datetime(2020, 1, 16), datetime(2020, 2, 16)],
            "provider_id": [200, 201],
        }
    )
    provider_df = pl.DataFrame(
        {
            "provider_id": [200, 201],
            "specialty_concept_id": [300, 400],
            "care_site_id": pl.Series([None, None], dtype=pl.Int64),
            "year_of_birth": pl.Series([None, None], dtype=pl.Int32),
            "gender_concept_id": pl.Series([None, None], dtype=pl.Int32),
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1970], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("provider", provider_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {
                        "ConditionOccurrence": {
                            "CodesetId": 1,
                            "CorrelatedCriteria": {
                                "Type": "ALL",
                                "CriteriaList": [
                                    {
                                        "Criteria": {
                                            "VisitOccurrence": {
                                                "ProviderSpecialty": [
                                                    {
                                                        "CONCEPT_ID": 300,
                                                        "CONCEPT_NAME": "specialty",
                                                        "STANDARD_CONCEPT": "S",
                                                    }
                                                ]
                                            }
                                        },
                                        "StartWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Coeff": 1},
                                            "UseEventEnd": False,
                                            "UseIndexEnd": False,
                                        },
                                        "EndWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Coeff": 1},
                                            "UseEventEnd": True,
                                            "UseIndexEnd": False,
                                        },
                                        "Occurrence": {"Type": 2, "Count": 1},
                                    }
                                ],
                            },
                        }
                    }
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_correlated_criteria_detect_events_extending_past_observation_period_end():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1, 2],
            "condition_start_date": [datetime(2024, 9, 12), datetime(2024, 12, 31)],
            "condition_end_date": pl.Series([datetime(2024, 9, 13), None], dtype=pl.Datetime),
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2024, 1, 1)],
            "observation_period_end_date": [datetime(2024, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "primary", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "correlated", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"ConditionOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 1, "Coeff": 1},
                            "UseEventEnd": False,
                            "UseIndexEnd": False,
                        },
                        "RestrictVisit": False,
                        "IgnoreObservationPeriod": False,
                        "Occurrence": {"Type": 1, "Count": 0},
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_primary_correlated_criteria_respect_observation_period_bounds():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1, 2],
            "condition_start_date": [datetime(2024, 11, 8), datetime(2024, 12, 31)],
            "condition_end_date": [datetime(2025, 2, 26), datetime(2025, 4, 20)],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2023, 2, 1)],
            "observation_period_end_date": [datetime(2024, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "primary", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "correlated", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {
                        "ConditionOccurrence": {
                            "CodesetId": 1,
                            "CorrelatedCriteria": {
                                "Type": "ALL",
                                "CriteriaList": [
                                    {
                                        "Criteria": {"ConditionOccurrence": {"CodesetId": 2}},
                                        "StartWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Days": 30, "Coeff": -1},
                                            "UseEventEnd": False,
                                            "UseIndexEnd": True,
                                        },
                                        "EndWindow": {
                                            "Start": {"Days": 30, "Coeff": 1},
                                            "End": {"Coeff": 1},
                                            "UseEventEnd": True,
                                            "UseIndexEnd": False,
                                        },
                                        "RestrictVisit": False,
                                        "IgnoreObservationPeriod": False,
                                        "Occurrence": {"Type": 2, "Count": 1},
                                    }
                                ],
                            },
                        }
                    }
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_correlated_visit_end_window_defaults_to_index_start():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 10)],
            "condition_end_date": [datetime(2020, 1, 12)],
            "visit_occurrence_id": [100],
        }
    )
    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [100],
            "person_id": [1],
            "visit_concept_id": [2],
            "visit_start_date": [datetime(2020, 1, 9)],
            "visit_end_date": [datetime(2020, 1, 10)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ANY",
                "CriteriaList": [
                    {
                        "Criteria": {"VisitOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Coeff": -1},
                            "End": {"Coeff": 1},
                            "UseEventEnd": False,
                            "UseIndexEnd": False,
                        },
                        "EndWindow": {
                            "Start": {"Days": 0, "Coeff": 1},
                            "End": {"Coeff": 1},
                            "UseEventEnd": True,
                            "UseIndexEnd": False,
                        },
                        "Occurrence": {"Type": 2, "Count": 1},
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_correlated_visit_end_window_allows_overflow_past_observation_end():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [1],
            "person_id": [1],
            "visit_concept_id": [1],
            "visit_start_date": [datetime(2020, 1, 10)],
            "visit_end_date": [datetime(2020, 1, 20)],
        }
    )
    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1],
            "person_id": [1],
            "condition_concept_id": [2],
            "condition_start_date": [datetime(2020, 1, 14)],
            "condition_end_date": [datetime(2020, 1, 20)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1970], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 12, 1)],
            "observation_period_end_date": [datetime(2020, 1, 15)],
        }
    )

    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {
                        "VisitOccurrence": {
                            "CodesetId": 1,
                            "CorrelatedCriteria": {
                                "Type": "ALL",
                                "CriteriaList": [
                                    {
                                        "Criteria": {"ConditionOccurrence": {"CodesetId": 2}},
                                        "StartWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Coeff": 1},
                                            "UseEventEnd": False,
                                        },
                                        "EndWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Days": 0, "Coeff": 1},
                                            "UseEventEnd": False,
                                            "UseIndexEnd": True,
                                        },
                                        "Occurrence": {"Type": 2, "Count": 1},
                                    }
                                ],
                            },
                        }
                    }
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    ctx = BuildContext(conn, CohortBuildOptions(), compile_codesets(conn, expression.concept_sets, CohortBuildOptions()))
    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_correlated_criteria_respect_ignore_observation_period_flag():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 2)],
        }
    )
    observation_df = pl.DataFrame(
        {
            "observation_id": [1],
            "person_id": [1],
            "observation_concept_id": [2],
            "observation_date": [datetime(2020, 2, 15)],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2020, 1, 31)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation", observation_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "observation", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"Observation": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": -1},
                            "End": {"Days": 60, "Coeff": 1},
                            "UseEventEnd": False,
                            "UseIndexEnd": False,
                        },
                        "IgnoreObservationPeriod": True,
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_inclusion_rule_condition_era_excludes_matching_events():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 10)],
            "condition_end_date": [datetime(2020, 1, 11)],
            "visit_occurrence_id": pl.Series([None], dtype=pl.Int64),
        }
    )
    condition_era_df = pl.DataFrame(
        {
            "condition_era_id": [1],
            "person_id": [1],
            "condition_concept_id": [2],
            "condition_occurrence_count": [0],
            "condition_era_start_date": [datetime(2020, 1, 5)],
            "condition_era_end_date": [datetime(2020, 1, 20)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {"person_id": [1], "observation_period_start_date": [datetime(2019, 1, 1)], "observation_period_end_date": [datetime(2021, 1, 1)]}
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("condition_era", condition_era_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "index", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "pregnancy", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "InclusionRules": [
                {
                    "name": "no pregnancy era",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [
                            {
                                "Criteria": {
                                    "ConditionEra": {
                                        "CodesetId": 2,
                                        "OccurrenceCount": {"Value": 0, "Op": "eq"},
                                    }
                                },
                                "StartWindow": {"Start": {"Coeff": -1}, "End": {"Days": 0, "Coeff": -1}, "UseIndexEnd": False},
                                "EndWindow": {"Start": {"Days": 0, "Coeff": 1}, "End": {"Coeff": 1}, "UseEventEnd": True},
                                "IgnoreObservationPeriod": True,
                                "Occurrence": {"Type": 0, "Count": 0},
                            }
                        ],
                    },
                }
            ],
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_correlated_occurrence_distinct_start_dates():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    condition_df = pl.DataFrame(
        {
            "condition_occurrence_id": [1, 2, 3],
            "person_id": [1, 1, 1],
            "condition_concept_id": [1, 2, 2],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "visit_occurrence_id": [1, 1, 1],
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
                {"id": 1, "name": "index", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "correlated", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "CriteriaList": [
                    {
                        "Criteria": {"ConditionOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": 0},
                            "End": {"Days": 10, "Coeff": 1},
                            "UseIndexEnd": False,
                        },
                        "IgnoreObservationPeriod": True,
                        "Occurrence": {"Type": 2, "Count": 2, "IsDistinct": True, "CountColumn": "START_DATE"},
                    }
                ],
            },
        }
    )

    options = CohortBuildOptions()
    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))

    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 1

    # collapse correlated occurrences to a single start date, which should fail the distinct requirement
    condition_df = condition_df.with_columns(pl.Series("condition_start_date", [datetime(2020, 1, 1)] * 3))
    conn.create_table("condition_occurrence", condition_df, overwrite=True)

    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))
    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 0


def test_correlated_window_respects_use_event_end():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1, 2],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2019, 12, 30)],
            "condition_end_date": [datetime(2020, 1, 3), datetime(2020, 1, 2)],
            "visit_occurrence_id": [1, 1],
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

    base_expression = {
        "ConceptSets": [
            {"id": 1, "name": "index", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
            {"id": 2, "name": "correlated", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
        ],
        "PrimaryCriteria": {
            "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
            "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
            "PrimaryCriteriaLimit": {"Type": "All"},
        },
        "AdditionalCriteria": {
            "Type": "ALL",
            "CriteriaList": [
                {
                    "Criteria": {"ConditionOccurrence": {"CodesetId": 2}},
                        "StartWindow": {
                            "Start": {"Days": 0, "Coeff": 0},
                            "UseEventEnd": True,
                        },
                    "IgnoreObservationPeriod": True,
                    "Occurrence": {"Type": 2, "Count": 1},
                }
            ],
        },
    }

    expression = CohortExpression.model_validate(base_expression)
    options = CohortBuildOptions()
    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))
    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 1, "Correlated window should use event end when flag is set"

    base_expression["AdditionalCriteria"]["CriteriaList"][0]["StartWindow"]["UseEventEnd"] = False
    expression = CohortExpression.model_validate(base_expression)
    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))
    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 0, "Without UseEventEnd the correlated event should be outside the window"


def test_visit_detail_correlated_requires_same_visit():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    visit_occurrence_df = pl.DataFrame(
        {
            "visit_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "visit_concept_id": [1, 1],
            "visit_start_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "visit_end_date": [datetime(2020, 1, 2), datetime(2020, 2, 2)],
        }
    )
    visit_detail_df = pl.DataFrame(
        {
            "visit_detail_id": [10],
            "person_id": [1],
            "visit_occurrence_id": [1],
            "visit_detail_concept_id": [2],
            "visit_detail_start_date": [datetime(2020, 1, 1)],
            "visit_detail_end_date": [datetime(2020, 1, 1)],
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

    conn.create_table("visit_occurrence", visit_occurrence_df, overwrite=True)
    conn.create_table("visit_detail", visit_detail_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "visit detail", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {
                        "VisitOccurrence": {
                            "CodesetId": 1,
                            "CorrelatedCriteria": {
                                "Type": "ALL",
                                "CriteriaList": [
                                    {
                                        "Criteria": {"VisitDetail": {"CodesetId": 2}},
                                        "StartWindow": {
                                            "Start": {"Days": 0, "Coeff": 0},
                                            "End": {"Days": 0, "Coeff": 1},
                                        },
                                    }
                                ],
                            },
                        }
                    }
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    options = CohortBuildOptions()
    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))
    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 1
    assert events.to_polars()["visit_occurrence_id"].to_list() == [1]

    # move the visit detail to a different visit_occurrence_id; no visit should satisfy the correlated block
    visit_detail_df = visit_detail_df.with_columns(pl.Series("visit_occurrence_id", [2]))
    conn.create_table("visit_detail", visit_detail_df, overwrite=True)
    ctx = BuildContext(conn, options, compile_codesets(conn, expression.concept_sets, options))
    events = build_primary_events(expression, ctx)
    assert events.count().execute() == 0



def test_expression_limit_keeps_first_included_event():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 3, 1)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 3, 2)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

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
            "QualifiedLimit": {"Type": "All"},
            "ExpressionLimit": {"Type": "First"},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert result.height == 1
    assert result.row(0, named=True)["start_date"] == datetime(2020, 1, 1)


def test_criteria_group_handles_demographics_and_counts():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_occurrence_id": [1, 2, 3],
            "person_id": [1, 1, 2],
            "condition_concept_id": [1, 1, 1],
            "condition_start_date": [
                datetime(2020, 1, 1),
                datetime(2020, 1, 10),
                datetime(2020, 3, 1),
            ],
            "condition_end_date": [
                datetime(2020, 1, 2),
                datetime(2020, 1, 11),
                datetime(2020, 3, 2),
            ],
            "visit_occurrence_id": [1, 2, 3],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [2],
            "measurement_date": [datetime(2020, 1, 10)],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "measurement_type_concept_id": [0],
            "visit_occurrence_id": [2],
        }
    )
    person_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "year_of_birth": [1950, 1995],
            "gender_concept_id": [8507, 8507],
            "race_concept_id": [0, 0],
            "ethnicity_concept_id": [0, 0],
        }
    )
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "observation_period_start_date": [datetime(2019, 1, 1), datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 12, 31), datetime(2021, 12, 31)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "measurement", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "ALL",
                "DemographicCriteriaList": [
                    {"Age": {"Op": "gte", "Value": 40}},
                ],
                "Groups": [
                    {
                        "Type": "AT_MOST",
                        "Count": 0,
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
                    }
                ],
            },
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    expression.collapse_settings = None
    assert expression.additional_criteria.demographic_criteria_list
    assert expression.additional_criteria.demographic_criteria_list[0].age is not None

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    result = events.to_polars().sort("event_id")

    assert len(result) == 1
    assert result["event_id"][0] == 1


def test_criteria_group_at_least_counts_matches():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [1, 1],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 10)],
            "condition_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 11)],
            "visit_occurrence_id": [1, 2],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [2],
            "measurement_date": [datetime(2020, 1, 10)],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "measurement_type_concept_id": [0],
            "visit_occurrence_id": [2],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507], "race_concept_id": [0], "ethnicity_concept_id": [0]})
    observation_period_df = pl.DataFrame({"person_id": [1], "observation_period_start_date": [datetime(2019, 1, 1)], "observation_period_end_date": [datetime(2021, 12, 31)]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "measurement", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "AdditionalCriteria": {
                "Type": "AT_LEAST",
                "Count": 1,
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
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    expression.collapse_settings = None

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    result = events.to_polars()

    assert len(result) == 1
    assert result["event_id"][0] == 2


def test_custom_era_end_strategy_extends_events():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 5)],
            "condition_end_date": [datetime(2020, 1, 6)],
            "visit_occurrence_id": [10],
        }
    )
    drug_exposure_df = pl.DataFrame(
        {
            "drug_exposure_id": [1, 2],
            "person_id": [1, 1],
            "drug_concept_id": [2, 2],
            "drug_source_concept_id": [2, 2],
            "drug_exposure_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 15)],
            "drug_exposure_end_date": [datetime(2020, 1, 10), datetime(2020, 1, 20)],
            "days_supply": [10, 10],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507], "race_concept_id": [0], "ethnicity_concept_id": [0]})
    observation_period_df = pl.DataFrame({"person_id": [1], "observation_period_start_date": [datetime(2019, 1, 1)], "observation_period_end_date": [datetime(2021, 12, 31)]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("drug_exposure", drug_exposure_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "drug", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "EndStrategy": {"CustomEra": {"DrugCodesetId": 2, "GapDays": 5, "Offset": 0}},
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    expression.collapse_settings = None

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    result = events.to_polars()

    assert len(result) == 1
    assert result["end_date"][0] == datetime(2020, 1, 20)


def test_inclusion_rule_counts_prior_events_with_open_start_window():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [101], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "condition_concept_id": [101, 101],
            "condition_start_date": [datetime(2019, 6, 1), datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2019, 6, 2), datetime(2020, 1, 2)],
            "visit_occurrence_id": [1, 2],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1950], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2018, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 101}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "InclusionRules": [
                {
                    "name": "no prior condition",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [
                            {
                                "Criteria": {"ConditionOccurrence": {"CodesetId": 1}},
                                "StartWindow": {
                                    "Start": {"Coeff": -1},
                                    "End": {"Days": 0, "Coeff": 1},
                                    "UseEventEnd": False,
                                },
                                "Occurrence": {"Type": 0, "Count": 0},
                            }
                        ],
                    },
                }
            ],
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    result = events.to_polars()

    assert result.is_empty(), "Events with a prior occurrence should be excluded when start window is open-ended"


def test_end_window_defaults_to_index_start_when_unspecified():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
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
            "condition_concept_id": [1],
            "condition_start_date": [datetime(2020, 1, 1)],
            "condition_end_date": [datetime(2020, 1, 10)],
        }
    )
    measurement_df = pl.DataFrame(
        {
            "measurement_id": [1],
            "person_id": [1],
            "measurement_concept_id": [2],
            "measurement_date": [datetime(2020, 1, 5)],
            "value_as_number": [1.0],
            "value_as_concept_id": [0],
            "unit_concept_id": [0],
            "measurement_type_concept_id": [0],
            "visit_occurrence_id": [1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("measurement", measurement_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "measurement", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "InclusionRules": [
                {
                    "name": "no delayed measurement",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [
                            {
                                "Criteria": {"Measurement": {"CodesetId": 2}},
                                "StartWindow": {"Start": {"Coeff": -1}, "End": {"Coeff": 1}},
                                "EndWindow": {"Start": {"Days": 1, "Coeff": 1}, "End": {"Coeff": 1}},
                                "Occurrence": {"Type": 0, "Count": 0},
                            }
                        ],
                    },
                }
            ],
            "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 0}},
            "CensoringCriteria": [],
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_correlated_criteria_apply_observation_window_when_missing():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}),
        overwrite=True,
    )
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    conn.create_table(
        "condition_occurrence",
        pl.DataFrame(
            {
                "condition_occurrence_id": [1],
                "person_id": [1],
                "condition_concept_id": [1],
                "condition_start_date": [datetime(2020, 1, 1)],
                "condition_end_date": [datetime(2020, 1, 2)],
            }
        ),
        overwrite=True,
    )
    conn.create_table(
        "procedure_occurrence",
        pl.DataFrame(
            {
                "procedure_occurrence_id": [10],
                "person_id": [1],
                "procedure_concept_id": [2],
                "procedure_date": [datetime(2019, 6, 1)],
                "procedure_end_date": [datetime(2019, 6, 2)],
            }
        ),
        overwrite=True,
    )
    conn.create_table(
        "observation_period",
        pl.DataFrame(
            {
                "person_id": [1],
                "observation_period_start_date": [datetime(2019, 1, 1)],
                "observation_period_end_date": [datetime(2021, 1, 1)],
            }
        ),
        overwrite=True,
    )

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "procedure", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [
                    {
                        "ConditionOccurrence": {
                            "CodesetId": 1,
                            "First": True,
                            "CorrelatedCriteria": {
                                "Type": "ALL",
                                "CriteriaList": [
                                    {
                                        "Criteria": {"ProcedureOccurrence": {"CodesetId": 2}},
                                        "StartWindow": {
                                            "Start": {"Coeff": -1},
                                            "End": {"Days": 180, "Coeff": -1},
                                            "UseEventEnd": False,
                                            "UseIndexEnd": False,
                                        },
                                        "RestrictVisit": False,
                                        "IgnoreObservationPeriod": False,
                                        "Occurrence": {"Type": 2, "Count": 1},
                                    }
                                ],
                            },
                        }
                    }
                ],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_custom_era_deduplicates_concept_and_source_exposures():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [10], "invalid_reason": [""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    drug_df = pl.DataFrame(
        {
            "drug_exposure_id": [1, 2],
            "person_id": [1, 1],
            "drug_concept_id": [10, 0],
            "drug_source_concept_id": [0, 10],
            "drug_exposure_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 1)],
            "drug_exposure_end_date": [datetime(2020, 1, 5), datetime(2020, 1, 5)],
            "days_supply": pl.Series([None, None], dtype=pl.Int64),
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    conn.create_table("drug_exposure", drug_df, overwrite=True)
    conn.create_table(
        "observation_period",
        pl.DataFrame(
            {
                "person_id": [1],
                "observation_period_start_date": [datetime(2019, 1, 1)],
                "observation_period_end_date": [datetime(2021, 1, 1)],
            }
        ),
        overwrite=True,
    )

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 0, "name": "drug", "expression": {"items": [{"concept": {"CONCEPT_ID": 10}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"DrugExposure": {"CodesetId": 0}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "EndStrategy": {"CustomEra": {"DrugCodesetId": 0, "GapDays": 60, "Offset": 0}},
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
            "ExpressionLimit": {"Type": "All"},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 1


def test_visit_detail_correlated_respects_restrict_visit_flag():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1, 2], "invalid_reason": ["", ""]})
    empty_int = pl.Series([], dtype=pl.Int64)
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table("concept_ancestor", pl.DataFrame({"ancestor_concept_id": empty_int, "descendant_concept_id": empty_int}), overwrite=True)
    conn.create_table(
        "concept_relationship",
        pl.DataFrame(
            {
                "concept_id_1": empty_int,
                "concept_id_2": empty_int,
                "relationship_id": pl.Series([], dtype=pl.String),
                "invalid_reason": pl.Series([], dtype=pl.String),
            }
        ),
        overwrite=True,
    )

    visit_df = pl.DataFrame(
        {
            "visit_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "visit_concept_id": [1, 1],
            "visit_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 3)],
            "visit_end_date": [datetime(2020, 1, 2), datetime(2020, 1, 4)],
            "visit_source_concept_id": [0, 0],
            "provider_id": [1, 1],
            "care_site_id": [1, 1],
        }
    )
    visit_detail_df = pl.DataFrame(
        {
            "visit_detail_id": [10],
            "person_id": [1],
            "visit_occurrence_id": [2],
            "visit_detail_concept_id": [2],
            "visit_detail_start_date": [datetime(2020, 1, 1)],
            "visit_detail_end_date": [datetime(2020, 1, 2)],
            "visit_detail_source_concept_id": [0],
            "visit_detail_type_concept_id": [0],
            "provider_id": [1],
            "care_site_id": [1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    observation_period_df = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [datetime(2019, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1)],
        }
    )

    conn.create_table("visit_occurrence", visit_df, overwrite=True)
    conn.create_table("visit_detail", visit_detail_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)
    conn.create_table("observation_period", observation_period_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "visit", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
                {"id": 2, "name": "detail", "expression": {"items": [{"concept": {"CONCEPT_ID": 2}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"VisitOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "All"},
            },
            "InclusionRules": [
                {
                    "name": "no disqualifying detail",
                    "expression": {
                        "Type": "ALL",
                        "CriteriaList": [
                            {
                                "Criteria": {"VisitDetail": {"CodesetId": 2}},
                                "StartWindow": {
                                    "Start": {"Days": 100, "Coeff": -1},
                                    "End": {"Days": 100, "Coeff": 1},
                                    "UseIndexEnd": False,
                                    "UseEventEnd": False,
                                },
                                "EndWindow": {
                                    "Start": {"Days": 100, "Coeff": -1},
                                    "End": {"Days": 100, "Coeff": 1},
                                    "UseIndexEnd": False,
                                    "UseEventEnd": True,
                                },
                                "RestrictVisit": False,
                                "Occurrence": {"Type": 0, "Count": 0},
                            }
                        ],
                    },
                }
            ],
            "EndStrategy": {"DateOffset": {"DateField": "EndDate", "Offset": 0}},
            "CensoringCriteria": [],
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    assert events.count().execute() == 0


def test_primary_limit_and_collapse_preserve_synthetic_ids():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_df = pl.DataFrame({"concept_id": [1], "invalid_reason": [""]})
    conn.create_table("concept", concept_df, overwrite=True)
    conn.create_table(
        "concept_ancestor",
        pl.DataFrame(
            {"ancestor_concept_id": pl.Series([], dtype=pl.Int64), "descendant_concept_id": pl.Series([], dtype=pl.Int64)}
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
            "condition_occurrence_id": [1, 2],
            "person_id": [1, 2],
            "condition_concept_id": [1, 1],
            "condition_start_date": [datetime(2020, 1, 1), datetime(2020, 1, 5)],
            "condition_end_date": [datetime(2020, 1, 3), datetime(2020, 1, 7)],
            "visit_occurrence_id": [10, 20],
        }
    )
    observation_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "observation_period_start_date": [datetime(2018, 1, 1), datetime(2018, 1, 1)],
            "observation_period_end_date": [datetime(2021, 1, 1), datetime(2021, 1, 1)],
        }
    )
    person_df = pl.DataFrame({"person_id": [1, 2], "year_of_birth": [1950, 1955], "gender_concept_id": [8507, 8507]})

    conn.create_table("condition_occurrence", condition_df, overwrite=True)
    conn.create_table("observation_period", observation_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    expression = CohortExpression.model_validate(
        {
            "ConceptSets": [
                {"id": 1, "name": "condition", "expression": {"items": [{"concept": {"CONCEPT_ID": 1}}]}},
            ],
            "PrimaryCriteria": {
                "CriteriaList": [{"ConditionOccurrence": {"CodesetId": 1}}],
                "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
                "PrimaryCriteriaLimit": {"Type": "First"},
            },
            "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        }
    )

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    df = events.to_polars().sort("person_id")

    assert len(df) == 2
    assert df["person_id"].to_list() == [1, 2]
    assert df["event_id"].to_list() == [1, 2], "Synthetic IDs must remain unique after primary-limit and collapse"
    assert "_source_event_id" not in df.columns, "Internal source ids should be dropped before collapse output"
