from __future__ import annotations

from datetime import date

from mitos.testing.fieldcases.harness import FieldCase
from mitos.testing.fieldcases.templates import generated_cases
from mitos.testing.omop.builder import OmopBuilder


def _base_cohort_expression(
    criteria_type: str, criteria_payload: dict, *, codeset_id: int = 1
) -> dict:
    return {
        "Title": f"FieldCase {criteria_type}",
        "PrimaryCriteria": {
            "CriteriaList": [
                {
                    criteria_type: {
                        **criteria_payload,
                        "CodesetId": codeset_id,
                    }
                }
            ],
            "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
            "PrimaryCriteriaLimit": {"Type": "All"},
        },
        "AdditionalCriteria": None,
        "ConceptSets": [
            {
                "id": codeset_id,
                "name": "codeset",
                "expression": {
                    "items": [
                        {
                            "concept": {"CONCEPT_ID": 1001},
                            "isExcluded": False,
                            "includeDescendants": False,
                            "includeMapped": False,
                        }
                    ]
                },
            }
        ],
        "QualifiedLimit": {"Type": "All"},
        "ExpressionLimit": {"Type": "All"},
        "InclusionRules": [],
        "EndStrategy": None,
        "CensoringCriteria": [],
        "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        "CensorWindow": {"StartDate": None, "EndDate": None},
    }


def _build_observation_value_as_string(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_observation(
        person_id=1,
        observation_concept_id=1001,
        observation_date=date(2000, 6, 1),
        value_as_string="POSITIVE",
    )
    builder.add_observation(
        person_id=1,
        observation_concept_id=1001,
        observation_date=date(2000, 6, 2),
        value_as_string="NEGATIVE",
    )


OBSERVATION_VALUE_AS_STRING_CONTAINS = FieldCase(
    name="observation_value_as_string_contains",
    cohort_json=_base_cohort_expression(
        "Observation",
        {"ValueAsString": {"Text": "POS", "Op": "contains"}},
    ),
    build_omop=_build_observation_value_as_string,
)


def _build_measurement_operator(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_measurement(
        person_id=1,
        measurement_concept_id=1001,
        measurement_date=date(2000, 6, 1),
        operator_concept_id=101,
        value_as_number=1.0,
        range_low=0.0,
        range_high=2.0,
    )
    builder.add_measurement(
        person_id=1,
        measurement_concept_id=1001,
        measurement_date=date(2000, 6, 2),
        operator_concept_id=102,
        value_as_number=1.0,
        range_low=0.0,
        range_high=2.0,
    )


MEASUREMENT_OPERATOR = FieldCase(
    name="measurement_operator",
    cohort_json=_base_cohort_expression(
        "Measurement",
        {"Operator": [{"CONCEPT_ID": 101}]},
    ),
    build_omop=_build_measurement_operator,
)


def _build_measurement_provider_specialty(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_provider(provider_id=10, specialty_concept_id=777)
    builder.add_provider(provider_id=11, specialty_concept_id=778)
    builder.add_measurement(
        person_id=1,
        measurement_concept_id=1001,
        measurement_date=date(2000, 6, 1),
        provider_id=10,
        value_as_number=1.0,
        range_low=0.0,
        range_high=2.0,
    )
    builder.add_measurement(
        person_id=1,
        measurement_concept_id=1001,
        measurement_date=date(2000, 6, 2),
        provider_id=11,
        value_as_number=1.0,
        range_low=0.0,
        range_high=2.0,
    )


MEASUREMENT_PROVIDER_SPECIALTY = FieldCase(
    name="measurement_provider_specialty",
    cohort_json=_base_cohort_expression(
        "Measurement",
        {"ProviderSpecialty": [{"CONCEPT_ID": 777}]},
    ),
    build_omop=_build_measurement_provider_specialty,
)


def _build_drug_exposure_stop_reason(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_drug_exposure(
        person_id=1,
        drug_concept_id=1001,
        drug_exposure_start_date=date(2000, 6, 1),
        stop_reason="KEPT",
    )
    builder.add_drug_exposure(
        person_id=1,
        drug_concept_id=1001,
        drug_exposure_start_date=date(2000, 6, 2),
        stop_reason="DROPPED",
    )


DRUG_EXPOSURE_STOP_REASON = FieldCase(
    name="drug_exposure_stop_reason",
    cohort_json=_base_cohort_expression(
        "DrugExposure",
        {"StopReason": {"Text": "KEP", "Op": "contains"}},
    ),
    build_omop=_build_drug_exposure_stop_reason,
)


def _build_device_exposure_unique_device_id(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_device_exposure(
        person_id=1,
        device_concept_id=1001,
        device_exposure_start_date=date(2000, 6, 1),
        unique_device_id="ABC-123",
    )
    builder.add_device_exposure(
        person_id=1,
        device_concept_id=1001,
        device_exposure_start_date=date(2000, 6, 2),
        unique_device_id="XYZ-999",
    )


DEVICE_EXPOSURE_UNIQUE_DEVICE_ID = FieldCase(
    name="device_exposure_unique_device_id",
    cohort_json=_base_cohort_expression(
        "DeviceExposure",
        {"UniqueDeviceId": {"Text": "ABC", "Op": "contains"}},
    ),
    build_omop=_build_device_exposure_unique_device_id,
)


def _build_death_occurrence_start_date(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1, year_of_birth=1980)
    builder.add_observation_period(
        person_id=1, start_date=date(1999, 1, 1), end_date=date(2002, 1, 1)
    )
    builder.add_death(person_id=1, death_date=date(1999, 6, 1), cause_concept_id=1001)
    builder.add_person(person_id=2, year_of_birth=1980)
    builder.add_observation_period(
        person_id=2, start_date=date(1999, 1, 1), end_date=date(2002, 1, 1)
    )
    builder.add_death(person_id=2, death_date=date(2000, 6, 1), cause_concept_id=1001)


DEATH_OCCURRENCE_START_DATE = FieldCase(
    name="death_occurrence_start_date",
    cohort_json=_base_cohort_expression(
        "Death",
        {"OccurrenceStartDate": {"Value": "2000-01-01", "Op": "gte"}},
    ),
    build_omop=_build_death_occurrence_start_date,
)


def _build_procedure_provider_specialty(builder: OmopBuilder) -> None:
    builder.add_person(person_id=1)
    builder.add_observation_period(
        person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
    )
    builder.add_provider(provider_id=10, specialty_concept_id=777)
    builder.add_provider(provider_id=11, specialty_concept_id=778)
    builder.add_procedure_occurrence(
        person_id=1,
        procedure_concept_id=1001,
        procedure_date=date(2000, 6, 1),
        provider_id=10,
    )
    builder.add_procedure_occurrence(
        person_id=1,
        procedure_concept_id=1001,
        procedure_date=date(2000, 6, 2),
        provider_id=11,
    )


PROCEDURE_PROVIDER_SPECIALTY = FieldCase(
    name="procedure_provider_specialty",
    cohort_json=_base_cohort_expression(
        "ProcedureOccurrence",
        {"ProviderSpecialty": [{"CONCEPT_ID": 777}]},
    ),
    build_omop=_build_procedure_provider_specialty,
)


ALL = [
    OBSERVATION_VALUE_AS_STRING_CONTAINS,
    MEASUREMENT_OPERATOR,
    MEASUREMENT_PROVIDER_SPECIALTY,
    DRUG_EXPOSURE_STOP_REASON,
    DEVICE_EXPOSURE_UNIQUE_DEVICE_ID,
    DEATH_OCCURRENCE_START_DATE,
    PROCEDURE_PROVIDER_SPECIALTY,
    *generated_cases(),
]
