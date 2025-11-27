from __future__ import annotations

from pathlib import Path

import polars as pl
import ibis
import pytest

from ibis_cohort.build_context import BuildContext, CohortBuildOptions, compile_codesets
from ibis_cohort.builders.pipeline import build_primary_events
from ibis_cohort.cohort_expression import CohortExpression


PHENOTYPE_SAMPLE = [
    Path("fixtures/phenotypes/phenotype-2.json"),
    Path("fixtures/phenotypes/phenotype-30.json"),
    Path("fixtures/phenotypes/phenotype-78.json"),
]


CDM_SCHEMAS = {
    "person": {
        "person_id": "int64",
        "year_of_birth": "int64",
        "gender_concept_id": "int64",
        "race_concept_id": "int64",
        "ethnicity_concept_id": "int64",
    },
    "observation_period": {
        "observation_period_id": "int64",
        "person_id": "int64",
        "observation_period_start_date": "datetime64[ns]",
        "observation_period_end_date": "datetime64[ns]",
        "period_type_concept_id": "int64",
    },
    "condition_occurrence": {
        "condition_occurrence_id": "int64",
        "person_id": "int64",
        "condition_concept_id": "int64",
        "condition_start_date": "datetime64[ns]",
        "condition_end_date": "datetime64[ns]",
        "condition_type_concept_id": "int64",
        "visit_occurrence_id": "int64",
        "condition_status_concept_id": "int64",
        "condition_source_concept_id": "int64",
        "provider_id": "int64",
        "stop_reason": "string",
    },
    "condition_era": {
        "condition_era_id": "int64",
        "person_id": "int64",
        "condition_concept_id": "int64",
        "condition_era_start_date": "datetime64[ns]",
        "condition_era_end_date": "datetime64[ns]",
        "condition_occurrence_count": "int64",
    },
    "drug_exposure": {
        "drug_exposure_id": "int64",
        "person_id": "int64",
        "drug_concept_id": "int64",
        "drug_exposure_start_date": "datetime64[ns]",
        "drug_exposure_end_date": "datetime64[ns]",
        "drug_type_concept_id": "int64",
        "route_concept_id": "int64",
        "quantity": "float64",
        "days_supply": "int64",
        "refills": "int64",
        "visit_occurrence_id": "int64",
    },
    "drug_era": {
        "drug_era_id": "int64",
        "person_id": "int64",
        "drug_concept_id": "int64",
        "drug_era_start_date": "datetime64[ns]",
        "drug_era_end_date": "datetime64[ns]",
        "drug_exposure_count": "int64",
        "gap_days": "int64",
    },
    "dose_era": {
        "dose_era_id": "int64",
        "person_id": "int64",
        "drug_concept_id": "int64",
        "dose_era_start_date": "datetime64[ns]",
        "dose_era_end_date": "datetime64[ns]",
        "unit_concept_id": "int64",
        "dose_value": "float64",
    },
    "visit_occurrence": {
        "visit_occurrence_id": "int64",
        "person_id": "int64",
        "visit_concept_id": "int64",
        "visit_start_date": "datetime64[ns]",
        "visit_end_date": "datetime64[ns]",
        "visit_type_concept_id": "int64",
        "visit_source_concept_id": "int64",
        "place_of_service_concept_id": "int64",
        "visit_length": "int64",
    },
    "measurement": {
        "measurement_id": "int64",
        "person_id": "int64",
        "measurement_concept_id": "int64",
        "measurement_date": "datetime64[ns]",
        "measurement_type_concept_id": "int64",
        "value_as_number": "float64",
        "value_as_concept_id": "int64",
        "unit_concept_id": "int64",
        "range_low": "float64",
        "range_high": "float64",
        "visit_occurrence_id": "int64",
    },
    "observation": {
        "observation_id": "int64",
        "person_id": "int64",
        "observation_concept_id": "int64",
        "observation_date": "datetime64[ns]",
        "observation_type_concept_id": "int64",
        "qualifier_concept_id": "int64",
        "unit_concept_id": "int64",
        "value_as_number": "float64",
        "value_as_concept_id": "int64",
        "value_as_string": "string",
        "visit_occurrence_id": "int64",
    },
    "device_exposure": {
        "device_exposure_id": "int64",
        "person_id": "int64",
        "device_concept_id": "int64",
        "device_exposure_start_date": "datetime64[ns]",
        "device_exposure_end_date": "datetime64[ns]",
        "device_type_concept_id": "int64",
        "visit_occurrence_id": "int64",
        "quantity": "float64",
    },
    "procedure_occurrence": {
        "procedure_occurrence_id": "int64",
        "person_id": "int64",
        "procedure_concept_id": "int64",
        "procedure_date": "datetime64[ns]",
        "procedure_type_concept_id": "int64",
        "modifier_concept_id": "int64",
        "quantity": "float64",
        "visit_occurrence_id": "int64",
        "procedure_source_concept_id": "int64",
    },
    "death": {
        "person_id": "int64",
        "death_date": "datetime64[ns]",
        "death_type_concept_id": "int64",
        "cause_concept_id": "int64",
    },
    "specimen": {
        "specimen_id": "int64",
        "person_id": "int64",
        "specimen_concept_id": "int64",
        "specimen_date": "datetime64[ns]",
        "specimen_type_concept_id": "int64",
        "quantity": "float64",
        "unit_concept_id": "int64",
        "anatomic_site_concept_id": "int64",
        "disease_status_concept_id": "int64",
        "specimen_source_id": "string",
    },
    "visit_detail": {
        "visit_detail_id": "int64",
        "person_id": "int64",
        "visit_detail_concept_id": "int64",
        "visit_occurrence_id": "int64",
        "visit_detail_start_date": "datetime64[ns]",
        "visit_detail_end_date": "datetime64[ns]",
        "visit_detail_type_concept_id": "int64",
        "visit_detail_source_concept_id": "int64",
        "provider_id": "int64",
        "care_site_id": "int64",
    },
    "payer_plan_period": {
        "payer_plan_period_id": "int64",
        "person_id": "int64",
        "payer_plan_period_start_date": "datetime64[ns]",
        "payer_plan_period_end_date": "datetime64[ns]",
        "payer_concept_id": "int64",
        "plan_concept_id": "int64",
        "sponsor_concept_id": "int64",
        "stop_reason_concept_id": "int64",
        "payer_source_concept_id": "int64",
        "plan_source_concept_id": "int64",
        "sponsor_source_concept_id": "int64",
        "stop_reason_source_concept_id": "int64",
    },
    "care_site": {
        "care_site_id": "int64",
        "place_of_service_concept_id": "int64",
        "location_id": "int64",
    },
    "provider": {
        "provider_id": "int64",
        "specialty_concept_id": "int64",
    },
    "location_history": {
        "location_history_id": "int64",
        "location_id": "int64",
        "entity_id": "int64",
        "domain_id": "string",
        "start_date": "datetime64[ns]",
        "end_date": "datetime64[ns]",
    },
    "location": {
        "location_id": "int64",
        "region_concept_id": "int64",
    },
}


VOCAB_TABLE_SCHEMAS = {
    "concept": {
        "concept_id": "int64",
        "invalid_reason": "string",
    },
    "concept_ancestor": {
        "ancestor_concept_id": "int64",
        "descendant_concept_id": "int64",
    },
    "concept_relationship": {
        "concept_id_1": "int64",
        "concept_id_2": "int64",
        "relationship_id": "string",
        "invalid_reason": "string",
    },
}


TYPE_MAP = {
    "int64": pl.Int64,
    "float64": pl.Float64,
    "string": pl.String,
    "datetime64[ns]": pl.Datetime,
}


def empty_table(schema: dict[str, str]) -> pl.DataFrame:
    columns = {
        col: pl.Series([], dtype=TYPE_MAP[dtype])
        for col, dtype in schema.items()
    }
    return pl.DataFrame(columns)


def register_cdm_tables(conn):
    for name, schema in CDM_SCHEMAS.items():
        conn.create_table(name, empty_table(schema), overwrite=True)


def register_vocab_tables(conn, concept_ids):
    if concept_ids:
        concept_df = pl.DataFrame(
            {
                "concept_id": sorted(concept_ids),
                "invalid_reason": [None] * len(concept_ids),
            },
            schema={"concept_id": pl.Int64, "invalid_reason": pl.String},
        )
    else:
        concept_df = empty_table(VOCAB_TABLE_SCHEMAS["concept"])
    conn.create_table("concept", concept_df, overwrite=True)
    for name, schema in VOCAB_TABLE_SCHEMAS.items():
        if name == "concept":
            continue
        conn.create_table(name, empty_table(schema), overwrite=True)


def gather_concept_ids(expression: CohortExpression):
    ids = set()
    for concept_set in expression.concept_sets:
        if concept_set.expression:
            for item in concept_set.expression.items:
                if item.concept and item.concept.concept_id is not None:
                    ids.add(int(item.concept.concept_id))
    return ids


@pytest.mark.parametrize("cohort_path", PHENOTYPE_SAMPLE, ids=[p.name for p in PHENOTYPE_SAMPLE])
def test_pipeline_executes_for_sample_phenotypes(cohort_path: Path):
    expression = CohortExpression.model_validate_json(cohort_path.read_text())
    conn = ibis.duckdb.connect(database=":memory:")
    register_cdm_tables(conn)
    register_vocab_tables(conn, gather_concept_ids(expression))

    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    assert events is not None
    result = events.to_polars()
    assert set(result.columns) == {"person_id", "event_id", "start_date", "end_date", "visit_occurrence_id"}
