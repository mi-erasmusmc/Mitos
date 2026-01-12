from __future__ import annotations

from datetime import date

from mitos.testing.fieldcases.harness import FieldCase
from mitos.testing.omop.builder import OmopBuilder


def _concept_set(*, codeset_id: int, concept_id: int) -> dict:
    return {
        "id": codeset_id,
        "name": f"codeset_{codeset_id}",
        "expression": {
            "items": [
                {
                    "concept": {"CONCEPT_ID": concept_id},
                    "isExcluded": False,
                    "includeDescendants": False,
                    "includeMapped": False,
                }
            ]
        },
    }


def _base_cohort_expression(
    criteria_type: str,
    criteria_payload: dict,
    *,
    codeset_id: int = 1,
    primary_concept_id: int = 1001,
    extra_concept_sets: list[dict] | None = None,
    include_primary_codeset: bool = True,
) -> dict:
    """
    Minimal Circe cohort JSON that isolates a single primary criterion.

    `codeset_id` maps to a single concept_id=1001 in ConceptSets and is expected
    to align with OMOP rows created by the FieldCase.
    """
    return {
        "Title": f"FieldCase {criteria_type}",
        "PrimaryCriteria": {
            "CriteriaList": [
                {
                    criteria_type: {
                        **criteria_payload,
                        **({"CodesetId": codeset_id} if include_primary_codeset else {}),
                    }
                }
            ],
            "ObservationWindow": {"PriorDays": 0, "PostDays": 0},
            "PrimaryCriteriaLimit": {"Type": "All"},
        },
        "AdditionalCriteria": None,
        "ConceptSets": [
            *(
                [_concept_set(codeset_id=codeset_id, concept_id=primary_concept_id)]
                if include_primary_codeset
                else []
            ),
            *(extra_concept_sets or []),
        ],
        "QualifiedLimit": {"Type": "All"},
        "ExpressionLimit": {"Type": "All"},
        "InclusionRules": [],
        "EndStrategy": None,
        "CensoringCriteria": [],
        "CollapseSettings": {"CollapseType": "ERA", "EraPad": 0},
        "CensorWindow": {"StartDate": None, "EndDate": None},
    }


def _wide_window(*, use_index_end: bool | None = False, use_event_end: bool | None = False) -> dict:
    return {
        "Start": {"Days": 36500, "Coeff": -1},
        "End": {"Days": 36500, "Coeff": 1},
        "UseIndexEnd": use_index_end,
        "UseEventEnd": use_event_end,
    }


def _correlated_criteria_item(
    *,
    criteria: dict,
    occurrence: dict | None = None,
    start_window: dict | None = None,
    end_window: dict | None = None,
    restrict_visit: bool | None = None,
    ignore_observation_period: bool | None = None,
) -> dict:
    payload: dict = {
        "Criteria": criteria,
        "StartWindow": start_window or _wide_window(),
        "EndWindow": end_window or _wide_window(),
        "Occurrence": occurrence or {"Type": 2, "Count": 1},
    }
    if restrict_visit is not None:
        payload["RestrictVisit"] = restrict_visit
    if ignore_observation_period is not None:
        payload["IgnoreObservationPeriod"] = ignore_observation_period
    return payload


def observation_value_as_string_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
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
        builder.add_observation(
            person_id=1,
            observation_concept_id=1001,
            observation_date=date(2000, 6, 3),
            value_as_string=None,
        )

    return [
        FieldCase(
            name="observation_value_as_string_contains",
            cohort_json=_base_cohort_expression(
                "Observation",
                {"ValueAsString": {"Text": "POS", "Op": "contains"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="observation_value_as_string_not_contains",
            cohort_json=_base_cohort_expression(
                "Observation",
                {"ValueAsString": {"Text": "POS", "Op": "!contains"}},
            ),
            build_omop=build,
        ),
    ]


def drug_exposure_stop_reason_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
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
        builder.add_drug_exposure(
            person_id=1,
            drug_concept_id=1001,
            drug_exposure_start_date=date(2000, 6, 3),
            stop_reason=None,
        )

    return [
        FieldCase(
            name="drug_exposure_stop_reason_contains",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"StopReason": {"Text": "KEP", "Op": "contains"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_stop_reason_not_contains",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"StopReason": {"Text": "KEP", "Op": "!contains"}},
            ),
            build_omop=build,
        ),
    ]


def device_exposure_unique_device_id_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
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
        builder.add_device_exposure(
            person_id=1,
            device_concept_id=1001,
            device_exposure_start_date=date(2000, 6, 3),
            unique_device_id=None,
        )

    return [
        FieldCase(
            name="device_exposure_unique_device_id_contains",
            cohort_json=_base_cohort_expression(
                "DeviceExposure",
                {"UniqueDeviceId": {"Text": "ABC", "Op": "contains"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="device_exposure_unique_device_id_not_contains",
            cohort_json=_base_cohort_expression(
                "DeviceExposure",
                {"UniqueDeviceId": {"Text": "ABC", "Op": "!contains"}},
            ),
            build_omop=build,
        ),
    ]


def death_occurrence_start_date_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1, year_of_birth=1980)
        builder.add_observation_period(
            person_id=1, start_date=date(1999, 1, 1), end_date=date(2002, 1, 1)
        )
        builder.add_death(person_id=1, death_date=date(1999, 12, 31), cause_concept_id=1001)

        builder.add_person(person_id=2, year_of_birth=1980)
        builder.add_observation_period(
            person_id=2, start_date=date(1999, 1, 1), end_date=date(2002, 1, 1)
        )
        builder.add_death(person_id=2, death_date=date(2000, 1, 1), cause_concept_id=1001)

    return [
        FieldCase(
            name="death_occurrence_start_date_gte_boundary",
            cohort_json=_base_cohort_expression(
                "Death",
                {"OccurrenceStartDate": {"Value": "2000-01-01", "Op": "gte"}},
            ),
            build_omop=build,
        )
    ]


def measurement_range_high_ratio_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        # denominator is 0 -> ratio NULL via Circe NULLIF; should not satisfy predicates
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 1),
            value_as_number=1.0,
            range_high=0.0,
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        # ratio = 1 / 2 = 0.5
        builder.add_measurement(
            person_id=2,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 2),
            value_as_number=1.0,
            range_high=2.0,
        )
        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        # ratio = 1 / 4 = 0.25
        builder.add_measurement(
            person_id=3,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 2),
            value_as_number=1.0,
            range_high=4.0,
        )

    return [
        FieldCase(
            name="measurement_range_high_ratio_lte",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"RangeHighRatio": {"Value": 0.6, "Op": "lte"}},
            ),
            build_omop=build,
        )
        ,
        FieldCase(
            name="measurement_range_high_ratio_gt_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"RangeHighRatio": {"Value": 0.3, "Op": "gt"}},
            ),
            build_omop=build,
        ),
    ]


def measurement_range_high_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(
            person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1)
        )
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 1),
            value_as_number=1.0,
            range_high=5.0,
        )
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 2),
            value_as_number=1.0,
            range_high=6.0,
        )
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 3),
            value_as_number=1.0,
            range_high=10.0,
        )

    return [
        FieldCase(
            name="measurement_range_high_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"RangeHigh": {"Value": 6.0, "Op": "gt"}},
            ),
            build_omop=build,
        )
        ,
        FieldCase(
            name="measurement_range_high_gte_boundary",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"RangeHigh": {"Value": 6.0, "Op": "gte"}},
            ),
            build_omop=build,
        ),
    ]


def procedure_occurrence_procedure_source_concept_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_procedure_occurrence(
            person_id=1,
            procedure_concept_id=1001,
            procedure_date=date(2000, 6, 1),
            procedure_source_concept_id=111,
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_procedure_occurrence(
            person_id=2,
            procedure_concept_id=1001,
            procedure_date=date(2000, 6, 2),
            procedure_source_concept_id=222,
        )
        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_procedure_occurrence(
            person_id=3,
            procedure_concept_id=1001,
            procedure_date=date(2000, 6, 3),
            procedure_source_concept_id=0,
        )

    return [
        FieldCase(
            name="procedure_occurrence_procedure_source_concept_eq_discriminator",
            cohort_json=_base_cohort_expression(
                "ProcedureOccurrence",
                {"ProcedureSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=111)],
            ),
            build_omop=build,
        )
        ,
        FieldCase(
            name="procedure_occurrence_procedure_source_concept_eq_discriminator_2",
            cohort_json=_base_cohort_expression(
                "ProcedureOccurrence",
                {"ProcedureSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=222)],
            ),
            build_omop=build,
        ),
    ]


def visit_occurrence_visit_source_concept_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=1001,
            visit_source_concept_id=555,
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_occurrence(
            person_id=2,
            visit_start_date=date(2000, 6, 2),
            visit_end_date=date(2000, 6, 2),
            visit_concept_id=1001,
            visit_source_concept_id=666,
        )
        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_occurrence(
            person_id=3,
            visit_start_date=date(2000, 6, 3),
            visit_end_date=date(2000, 6, 3),
            visit_concept_id=1001,
            visit_source_concept_id=0,
        )

    return [
        FieldCase(
            name="visit_occurrence_visit_source_concept_eq_discriminator",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"VisitSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=555)],
            ),
            build_omop=build,
        )
        ,
        FieldCase(
            name="visit_occurrence_visit_source_concept_eq_discriminator_2",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"VisitSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=666)],
            ),
            build_omop=build,
        ),
    ]


def visit_detail_visit_detail_source_concept_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_detail(
            person_id=1,
            visit_detail_concept_id=1001,
            visit_detail_start_date=date(2000, 6, 1),
            visit_detail_end_date=date(2000, 6, 1),
            visit_detail_source_concept_id=7001,
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_detail(
            person_id=2,
            visit_detail_concept_id=1001,
            visit_detail_start_date=date(2000, 6, 2),
            visit_detail_end_date=date(2000, 6, 2),
            visit_detail_source_concept_id=7002,
        )
        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_detail(
            person_id=3,
            visit_detail_concept_id=1001,
            visit_detail_start_date=date(2000, 6, 3),
            visit_detail_end_date=date(2000, 6, 3),
            visit_detail_source_concept_id=0,
        )

    return [
        FieldCase(
            name="visit_detail_source_concept_eq_discriminator",
            cohort_json=_base_cohort_expression(
                "VisitDetail",
                {"VisitDetailSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=7001)],
            ),
            build_omop=build,
        )
        ,
        FieldCase(
            name="visit_detail_source_concept_eq_discriminator_2",
            cohort_json=_base_cohort_expression(
                "VisitDetail",
                {"VisitDetailSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=7002)],
            ),
            build_omop=build,
        ),
    ]

def visit_detail_codeset_id_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_detail(
            person_id=1,
            visit_detail_concept_id=1001,
            visit_detail_start_date=date(2000, 6, 1),
            visit_detail_end_date=date(2000, 6, 1),
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_visit_detail(
            person_id=2,
            visit_detail_concept_id=2002,
            visit_detail_start_date=date(2000, 6, 2),
            visit_detail_end_date=date(2000, 6, 2),
        )

    return [
        FieldCase(
            name="visit_detail_codeset_id_discriminator",
            cohort_json=_base_cohort_expression("VisitDetail", {}),
            build_omop=build,
        ),
        FieldCase(
            name="visit_detail_codeset_id_discriminator_2",
            cohort_json=_base_cohort_expression(
                "VisitDetail",
                {},
                codeset_id=2,
                primary_concept_id=2002,
            ),
            build_omop=build,
        ),
    ]

def condition_occurrence_condition_source_concept_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_source_concept_id=9001,
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 2),
            condition_source_concept_id=9002,
        )

    return [
        FieldCase(
            name="condition_occurrence_source_concept_codeset_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=9001)],
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_source_concept_codeset_discriminator_2",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=9002)],
            ),
            build_omop=build,
        ),
    ]

def condition_occurrence_condition_status_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_status_concept_id=111,
        )

        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(
            person_id=2,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_status_concept_id=222,
        )

    return [
        FieldCase(
            name="condition_occurrence_condition_status_concept_list_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionStatus": [{"CONCEPT_ID": 111}]},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_condition_status_concept_list_discriminator_2",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionStatus": [{"CONCEPT_ID": 222}]},
            ),
            build_omop=build,
        ),
    ]


def condition_occurrence_condition_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_type_concept_id=111,
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 2),
            condition_type_concept_id=222,
        )

    return [
        FieldCase(
            name="condition_occurrence_condition_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionType": [{"CONCEPT_ID": 111}], "ConditionTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_condition_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"ConditionType": [{"CONCEPT_ID": 111}], "ConditionTypeExclude": False},
            ),
            build_omop=build,
        ),
    ]


def condition_era_occurrence_count_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_era(
            person_id=1,
            condition_concept_id=1001,
            condition_era_start_date=date(2000, 6, 1),
            condition_era_end_date=date(2000, 6, 10),
            condition_occurrence_count=1,
        )
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_era(
            person_id=2,
            condition_concept_id=1001,
            condition_era_start_date=date(2000, 7, 1),
            condition_era_end_date=date(2000, 7, 10),
            condition_occurrence_count=2,
        )
        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_era(
            person_id=3,
            condition_concept_id=1001,
            condition_era_start_date=date(2000, 8, 1),
            condition_era_end_date=date(2000, 8, 10),
            condition_occurrence_count=3,
        )

    return [
        FieldCase(
            name="condition_era_occurrence_count_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionEra",
                {"OccurrenceCount": {"Value": 2, "Op": "gte"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_era_occurrence_count_gt_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionEra",
                {"OccurrenceCount": {"Value": 2, "Op": "gt"}},
            ),
            build_omop=build,
        ),
    ]

def numeric_range_extent_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 1),
            value_as_number=5.0,
            range_low=0.0,
        )

        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_measurement(
            person_id=2,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 1),
            value_as_number=11.0,
            range_low=0.0,
        )

    return [
        FieldCase(
            name="numeric_range_between_uses_extent",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"ValueAsNumber": {"Value": 0.0, "Op": "bt", "Extent": 10.0}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="numeric_range_between_uses_extent_boundary",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"ValueAsNumber": {"Value": 5.0, "Op": "bt", "Extent": 5.0}},
            ),
            build_omop=build,
        ),
    ]


def criteria_group_count_demographic_age_gender_cases() -> list[FieldCase]:
    def base_expr(*, count: int) -> dict:
        expr = _base_cohort_expression("ConditionOccurrence", {})
        expr["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
        expr["AdditionalCriteria"] = {
            "Type": "AT_LEAST",
            "Count": count,
            "CriteriaList": [],
            "DemographicCriteriaList": [
                {"Age": {"Value": 18, "Op": "gte"}},
                {"Gender": [{"CONCEPT_ID": 8507}]},
            ],
            "Groups": [],
        }
        return expr

    def build(builder: OmopBuilder) -> None:
        # Matches both Age and Gender
        builder.add_person(person_id=1, year_of_birth=1980, gender_concept_id=8507)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 1))

        # Matches Age only
        builder.add_person(person_id=2, year_of_birth=1980, gender_concept_id=8532)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 6, 1))

        # Matches Gender only
        builder.add_person(person_id=3, year_of_birth=1995, gender_concept_id=8507)
        builder.add_observation_period(person_id=3, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=3, condition_concept_id=1001, condition_start_date=date(2000, 6, 1))

    return [
        FieldCase(
            name="criteria_group_at_least_2_demographic_age_gender",
            cohort_json=base_expr(count=2),
            build_omop=build,
        ),
        FieldCase(
            name="criteria_group_at_least_1_demographic_age_gender",
            cohort_json=base_expr(count=1),
            build_omop=build,
        ),
    ]


def drug_era_era_length_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_drug_era(
            person_id=1,
            drug_concept_id=1001,
            drug_era_start_date=date(2000, 6, 1),
            drug_era_end_date=date(2000, 6, 2),
        )
        builder.add_drug_era(
            person_id=1,
            drug_concept_id=1001,
            drug_era_start_date=date(2000, 7, 1),
            drug_era_end_date=date(2000, 7, 10),
        )

    return [
        FieldCase(
            name="drug_era_era_length_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugEra",
                {"EraLength": {"Value": 5, "Op": "gte"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="drug_era_era_length_gt_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugEra",
                {"EraLength": {"Value": 5, "Op": "gt"}},
            ),
            build_omop=build,
        ),
    ]


def dose_era_codeset_id_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_dose_era(
            person_id=1,
            drug_concept_id=1001,
            dose_era_start_date=date(2000, 6, 1),
            dose_era_end_date=date(2000, 6, 2),
        )
        builder.add_dose_era(
            person_id=1,
            drug_concept_id=2002,
            dose_era_start_date=date(2000, 6, 3),
            dose_era_end_date=date(2000, 6, 4),
        )

    return [
        FieldCase(
            name="dose_era_codeset_id_discriminator",
            cohort_json=_base_cohort_expression(
                "DoseEra",
                {},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="dose_era_codeset_id_discriminator_2",
            cohort_json=_base_cohort_expression(
                "DoseEra",
                {},
                codeset_id=2,
                primary_concept_id=2002,
            ),
            build_omop=build,
        ),
    ]

def condition_occurrence_first_age_gender_date_visit_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1, year_of_birth=1980, gender_concept_id=8507)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v1 = builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=1111,
        )
        v2 = builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 2),
            visit_end_date=date(2000, 6, 2),
            visit_concept_id=2222,
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_type_concept_id=111,
            visit_occurrence_id=v1,
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 2),
            condition_type_concept_id=111,
            visit_occurrence_id=v2,
        )

        builder.add_person(person_id=2, year_of_birth=1995, gender_concept_id=8532)
        builder.add_observation_period(person_id=2, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(
            person_id=2,
            condition_concept_id=1001,
            condition_start_date=date(2000, 6, 1),
            condition_type_concept_id=111,
            visit_occurrence_id=v1,
        )

    return [
        FieldCase(
            name="condition_occurrence_first_discriminator",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"First": True}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_first_false_keeps_all",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"First": False}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_age_gte_discriminator",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"Age": {"Value": 18, "Op": "gte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_age_lt_discriminator",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"Age": {"Value": 18, "Op": "lt"}}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_gender_discriminator",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"Gender": [{"CONCEPT_ID": 8507}]}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_gender_discriminator_2",
            cohort_json=_base_cohort_expression("ConditionOccurrence", {"Gender": [{"CONCEPT_ID": 8532}]}),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_occurrence_start_date_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"OccurrenceStartDate": {"Value": "2000-06-02", "Op": "gte"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_visit_type_discriminator",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"VisitType": [{"CONCEPT_ID": 1111}]},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="condition_occurrence_visit_type_discriminator_2",
            cohort_json=_base_cohort_expression(
                "ConditionOccurrence",
                {"VisitType": [{"CONCEPT_ID": 2222}]},
            ),
            build_omop=build,
        ),
    ]


def measurement_basic_field_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 1),
            measurement_type_concept_id=111,
            measurement_source_concept_id=9001,
            value_as_number=1.0,
            value_as_concept_id=1111,
            unit_concept_id=9529,
            range_low=0.0,
        )
        builder.add_measurement(
            person_id=1,
            measurement_concept_id=1001,
            measurement_date=date(2000, 6, 2),
            measurement_type_concept_id=222,
            measurement_source_concept_id=9002,
            value_as_number=10.0,
            value_as_concept_id=2222,
            unit_concept_id=3195625,
            range_low=5.0,
        )

    return [
        FieldCase(
            name="measurement_value_as_number_lte_discriminator",
            cohort_json=_base_cohort_expression("Measurement", {"ValueAsNumber": {"Value": 2.0, "Op": "lte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_value_as_number_gte_discriminator",
            cohort_json=_base_cohort_expression("Measurement", {"ValueAsNumber": {"Value": 2.0, "Op": "gte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_value_as_concept_discriminator",
            cohort_json=_base_cohort_expression("Measurement", {"ValueAsConcept": [{"CONCEPT_ID": 1111}]}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_value_as_concept_discriminator_2",
            cohort_json=_base_cohort_expression("Measurement", {"ValueAsConcept": [{"CONCEPT_ID": 2222}]}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_unit_discriminator",
            cohort_json=_base_cohort_expression("Measurement", {"Unit": [{"CONCEPT_ID": 9529}]}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_unit_discriminator_2",
            cohort_json=_base_cohort_expression("Measurement", {"Unit": [{"CONCEPT_ID": 3195625}]}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_range_low_gte_discriminator",
            cohort_json=_base_cohort_expression("Measurement", {"RangeLow": {"Value": 1.0, "Op": "gte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_occurrence_start_date_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"OccurrenceStartDate": {"Value": "2000-06-02", "Op": "gte"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_occurrence_start_date_gt_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"OccurrenceStartDate": {"Value": "2000-06-01", "Op": "gt"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_measurement_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"MeasurementType": [{"CONCEPT_ID": 111}], "MeasurementTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_measurement_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"MeasurementType": [{"CONCEPT_ID": 111}], "MeasurementTypeExclude": False},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_source_concept_codeset_discriminator",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"MeasurementSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=9001)],
            ),
            build_omop=build,
        ),
        FieldCase(
            name="measurement_source_concept_codeset_discriminator_2",
            cohort_json=_base_cohort_expression(
                "Measurement",
                {"MeasurementSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=9002)],
            ),
            build_omop=build,
        ),
    ]


def observation_basic_field_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_observation(
            person_id=1,
            observation_concept_id=1001,
            observation_date=date(2000, 6, 1),
            observation_type_concept_id=111,
            value_as_number=1.0,
            value_as_concept_id=1111,
            unit_concept_id=9529,
            observation_source_concept_id=9001,
        )
        builder.add_observation(
            person_id=1,
            observation_concept_id=1001,
            observation_date=date(2000, 6, 2),
            observation_type_concept_id=222,
            value_as_number=10.0,
            value_as_concept_id=2222,
            unit_concept_id=3195625,
            observation_source_concept_id=9002,
        )

    return [
        FieldCase(
            name="observation_first_discriminator",
            cohort_json=_base_cohort_expression("Observation", {"First": True}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_first_false_keeps_all",
            cohort_json=_base_cohort_expression("Observation", {"First": False}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_value_as_number_lte_discriminator",
            cohort_json=_base_cohort_expression("Observation", {"ValueAsNumber": {"Value": 2.0, "Op": "lte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_value_as_number_gte_discriminator",
            cohort_json=_base_cohort_expression("Observation", {"ValueAsNumber": {"Value": 2.0, "Op": "gte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_value_as_concept_discriminator",
            cohort_json=_base_cohort_expression("Observation", {"ValueAsConcept": [{"CONCEPT_ID": 1111}]}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_value_as_concept_discriminator_2",
            cohort_json=_base_cohort_expression("Observation", {"ValueAsConcept": [{"CONCEPT_ID": 2222}]}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_unit_discriminator",
            cohort_json=_base_cohort_expression("Observation", {"Unit": [{"CONCEPT_ID": 9529}]}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_unit_discriminator_2",
            cohort_json=_base_cohort_expression("Observation", {"Unit": [{"CONCEPT_ID": 3195625}]}),
            build_omop=build,
        ),
        FieldCase(
            name="observation_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "Observation",
                {"ObservationType": [{"CONCEPT_ID": 111}], "ObservationTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="observation_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "Observation",
                {"ObservationType": [{"CONCEPT_ID": 111}], "ObservationTypeExclude": False},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="observation_source_concept_codeset_discriminator",
            cohort_json=_base_cohort_expression(
                "Observation",
                {"ObservationSourceConcept": 2},
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=9001)],
            ),
            build_omop=build,
        ),
    ]


def drug_exposure_basic_field_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1, year_of_birth=1980)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_drug_exposure(
            person_id=1,
            drug_concept_id=1001,
            drug_exposure_start_date=date(2000, 6, 1),
            drug_type_concept_id=111,
        )
        builder.add_drug_exposure(
            person_id=1,
            drug_concept_id=1001,
            drug_exposure_start_date=date(2000, 6, 2),
            drug_type_concept_id=222,
        )

        builder.add_person(person_id=2, year_of_birth=2000)
        builder.add_observation_period(person_id=2, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_drug_exposure(
            person_id=2,
            drug_concept_id=1001,
            drug_exposure_start_date=date(2000, 6, 2),
            drug_type_concept_id=222,
        )

    return [
        FieldCase(
            name="drug_exposure_first_discriminator",
            cohort_json=_base_cohort_expression("DrugExposure", {"First": True}),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_first_false_keeps_all",
            cohort_json=_base_cohort_expression("DrugExposure", {"First": False}),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_occurrence_start_date_gte_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"OccurrenceStartDate": {"Value": "2000-06-02", "Op": "gte"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_occurrence_start_date_gt_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"OccurrenceStartDate": {"Value": "2000-06-01", "Op": "gt"}},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_age_gte_discriminator",
            cohort_json=_base_cohort_expression("DrugExposure", {"Age": {"Value": 18, "Op": "gte"}}),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_age_lt_discriminator",
            cohort_json=_base_cohort_expression("DrugExposure", {"Age": {"Value": 18, "Op": "lt"}}),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_drug_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"DrugType": [{"CONCEPT_ID": 111}], "DrugTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="drug_exposure_drug_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "DrugExposure",
                {"DrugType": [{"CONCEPT_ID": 111}], "DrugTypeExclude": False},
            ),
            build_omop=build,
        ),
    ]


def device_exposure_first_and_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_device_exposure(
            person_id=1,
            device_concept_id=1001,
            device_exposure_start_date=date(2000, 6, 1),
            device_type_concept_id=111,
        )
        builder.add_device_exposure(
            person_id=1,
            device_concept_id=1001,
            device_exposure_start_date=date(2000, 6, 2),
            device_type_concept_id=222,
        )

    return [
        FieldCase(
            name="device_exposure_first_discriminator",
            cohort_json=_base_cohort_expression("DeviceExposure", {"First": True}),
            build_omop=build,
        ),
        FieldCase(
            name="device_exposure_first_false_keeps_all",
            cohort_json=_base_cohort_expression("DeviceExposure", {"First": False}),
            build_omop=build,
        ),
        FieldCase(
            name="device_exposure_device_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "DeviceExposure",
                {"DeviceType": [{"CONCEPT_ID": 111}], "DeviceTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="device_exposure_device_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "DeviceExposure",
                {"DeviceType": [{"CONCEPT_ID": 111}], "DeviceTypeExclude": False},
            ),
            build_omop=build,
        ),
    ]


def procedure_occurrence_first_and_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_procedure_occurrence(
            person_id=1,
            procedure_concept_id=1001,
            procedure_date=date(2000, 6, 1),
            procedure_type_concept_id=111,
        )
        builder.add_procedure_occurrence(
            person_id=1,
            procedure_concept_id=1001,
            procedure_date=date(2000, 6, 2),
            procedure_type_concept_id=222,
        )

    return [
        FieldCase(
            name="procedure_occurrence_first_discriminator",
            cohort_json=_base_cohort_expression("ProcedureOccurrence", {"First": True}),
            build_omop=build,
        ),
        FieldCase(
            name="procedure_occurrence_first_false_keeps_all",
            cohort_json=_base_cohort_expression("ProcedureOccurrence", {"First": False}),
            build_omop=build,
        ),
        FieldCase(
            name="procedure_occurrence_procedure_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "ProcedureOccurrence",
                {"ProcedureType": [{"CONCEPT_ID": 111}], "ProcedureTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="procedure_occurrence_procedure_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "ProcedureOccurrence",
                {"ProcedureType": [{"CONCEPT_ID": 111}], "ProcedureTypeExclude": False},
            ),
            build_omop=build,
        ),
    ]


def specimen_codeset_and_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_specimen(
            person_id=1,
            specimen_concept_id=1001,
            specimen_date=date(2000, 6, 1),
            specimen_type_concept_id=111,
        )
        builder.add_specimen(
            person_id=1,
            specimen_concept_id=2002,
            specimen_date=date(2000, 6, 2),
            specimen_type_concept_id=222,
        )

    return [
        FieldCase(
            name="specimen_codeset_id_discriminator",
            cohort_json=_base_cohort_expression("Specimen", {}),
            build_omop=build,
        ),
        FieldCase(
            name="specimen_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "Specimen",
                {"SpecimenType": [{"CONCEPT_ID": 111}], "SpecimenTypeExclude": True},
                codeset_id=2,
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=2002)],
            ),
            build_omop=build,
        ),
    ]


def observation_period_user_defined_period_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2000, 12, 31))
        builder.add_observation_period(person_id=1, start_date=date(1990, 1, 1), end_date=date(1990, 12, 31))

    return [
        FieldCase(
            name="observation_period_user_defined_period_start_discriminator",
            cohort_json=_base_cohort_expression(
                "ObservationPeriod",
                {"UserDefinedPeriod": {"StartDate": "2000-06-01"}},
                codeset_id=1,
                include_primary_codeset=False,
            ),
            build_omop=build,
        ),
        FieldCase(
            name="observation_period_user_defined_period_start_discriminator_2",
            cohort_json=_base_cohort_expression(
                "ObservationPeriod",
                {"UserDefinedPeriod": {"StartDate": "1990-06-01"}},
                codeset_id=1,
                include_primary_codeset=False,
            ),
            build_omop=build,
        ),
    ]


def visit_occurrence_provider_specialty_and_visit_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_provider(provider_id=10, specialty_concept_id=777)
        builder.add_provider(provider_id=11, specialty_concept_id=778)
        builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=1001,
            visit_type_concept_id=1001,
            provider_id=10,
        )
        builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 2),
            visit_end_date=date(2000, 6, 2),
            visit_concept_id=1001,
            visit_type_concept_id=2002,
            provider_id=11,
        )
        builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 3),
            visit_end_date=date(2000, 6, 3),
            visit_concept_id=2002,
            visit_type_concept_id=1001,
            provider_id=10,
        )

    return [
        FieldCase(
            name="visit_occurrence_provider_specialty_discriminator",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"ProviderSpecialty": [{"CONCEPT_ID": 777}]},
                codeset_id=2,
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=1001)],
            ),
            build_omop=build,
        ),
        FieldCase(
            name="visit_occurrence_provider_specialty_discriminator_2",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"ProviderSpecialty": [{"CONCEPT_ID": 778}]},
                codeset_id=2,
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=1001)],
            ),
            build_omop=build,
        ),
        FieldCase(
            name="visit_occurrence_visit_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"VisitType": [{"CONCEPT_ID": 1001}], "VisitTypeExclude": True},
                codeset_id=2,
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=1001)],
            ),
            build_omop=build,
        ),
        FieldCase(
            name="visit_occurrence_visit_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "VisitOccurrence",
                {"VisitType": [{"CONCEPT_ID": 1001}], "VisitTypeExclude": False},
                codeset_id=2,
                extra_concept_sets=[_concept_set(codeset_id=2, concept_id=1001)],
            ),
            build_omop=build,
        ),
    ]


def death_death_type_exclude_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_death(person_id=1, death_date=date(2000, 6, 1), cause_concept_id=1001, death_type_concept_id=111)
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        builder.add_death(person_id=2, death_date=date(2000, 6, 2), cause_concept_id=1001, death_type_concept_id=222)

    return [
        FieldCase(
            name="death_type_exclude_discriminator",
            cohort_json=_base_cohort_expression(
                "Death",
                {"DeathType": [{"CONCEPT_ID": 111}], "DeathTypeExclude": True},
            ),
            build_omop=build,
        ),
        FieldCase(
            name="death_type_include_discriminator",
            cohort_json=_base_cohort_expression(
                "Death",
                {"DeathType": [{"CONCEPT_ID": 111}], "DeathTypeExclude": False},
            ),
            build_omop=build,
        ),
    ]


def occurrence_correlated_criteria_cases() -> list[FieldCase]:
    def base_expr(correlated: dict) -> dict:
        expr = _base_cohort_expression("ConditionOccurrence", {})
        primary = expr["PrimaryCriteria"]["CriteriaList"][0]["ConditionOccurrence"]
        primary["CorrelatedCriteria"] = {
            "Type": "ALL",
            "CriteriaList": [correlated],
            "DemographicCriteriaList": [],
            "Groups": [],
        }
        expr["ConceptSets"].extend(
            [
                _concept_set(codeset_id=2, concept_id=2002),
            ]
        )
        return expr

    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v1 = builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=0,
        )
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v1)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=2002, condition_start_date=date(2000, 6, 2), visit_occurrence_id=v1)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=2002, condition_start_date=date(2000, 6, 3), visit_occurrence_id=v1)

        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v2 = builder.add_visit_occurrence(
            person_id=2,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=0,
        )
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v2)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=2002, condition_start_date=date(2000, 6, 2), visit_occurrence_id=v2)

        builder.add_person(person_id=3)
        builder.add_observation_period(person_id=3, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v3 = builder.add_visit_occurrence(
            person_id=3,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=0,
        )
        v4 = builder.add_visit_occurrence(
            person_id=3,
            visit_start_date=date(2000, 6, 2),
            visit_end_date=date(2000, 6, 2),
            visit_concept_id=0,
        )
        builder.add_condition_occurrence(person_id=3, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v3)
        builder.add_condition_occurrence(person_id=3, condition_concept_id=2002, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v3)
        builder.add_condition_occurrence(person_id=3, condition_concept_id=2002, condition_start_date=date(2000, 6, 2), visit_occurrence_id=v4)

    return [
        FieldCase(
            name="occurrence_at_least_count_discriminator",
            cohort_json=base_expr(
                _correlated_criteria_item(
                    criteria={"ConditionOccurrence": {"CodesetId": 2}},
                    occurrence={"Type": 2, "Count": 2},
                )
            ),
            build_omop=build,
        ),
        FieldCase(
            name="occurrence_distinct_visit_id_discriminator",
            cohort_json=base_expr(
                _correlated_criteria_item(
                    criteria={"ConditionOccurrence": {"CodesetId": 2}},
                    occurrence={"Type": 2, "Count": 2, "IsDistinct": True, "CountColumn": "VISIT_ID"},
                )
            ),
            build_omop=build,
        ),
        FieldCase(
            name="occurrence_not_distinct_visit_id_discriminator",
            cohort_json=base_expr(
                _correlated_criteria_item(
                    criteria={"ConditionOccurrence": {"CodesetId": 2}},
                    occurrence={"Type": 2, "Count": 2, "IsDistinct": False, "CountColumn": "VISIT_ID"},
                )
            ),
            build_omop=build,
        ),
    ]


def correlated_window_missing_days_cases() -> list[FieldCase]:
    """
    Regression/edge cases for Circe Window.Endpoint where `Days` may be missing (null in Java).
    """

    def base_expr(*, correlated: dict) -> dict:
        expr = _base_cohort_expression("ConditionOccurrence", {})
        primary = expr["PrimaryCriteria"]["CriteriaList"][0]["ConditionOccurrence"]
        primary["CorrelatedCriteria"] = {
            "Type": "ALL",
            "CriteriaList": [correlated],
            "DemographicCriteriaList": [],
            "Groups": [],
        }
        expr["ConceptSets"].extend([_concept_set(codeset_id=2, concept_id=2002)])
        return expr

    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(
            person_id=1,
            start_date=date(1999, 1, 1),
            end_date=date(2000, 6, 1),
        )
        v1 = builder.add_visit_occurrence(
            person_id=1,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=0,
        )
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v1)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=2002, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v1)

        builder.add_person(person_id=2)
        builder.add_observation_period(
            person_id=2,
            start_date=date(1999, 1, 1),
            end_date=date(2000, 6, 2),
        )
        v2 = builder.add_visit_occurrence(
            person_id=2,
            visit_start_date=date(2000, 6, 1),
            visit_end_date=date(2000, 6, 1),
            visit_concept_id=0,
        )
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v2)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=2002, condition_start_date=date(2000, 6, 2), visit_occurrence_id=v2)

    correlated = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        start_window={
            "Start": {"Coeff": 1},
            "End": {"Days": 0, "Coeff": 1},
            "UseIndexEnd": False,
            "UseEventEnd": False,
        },
        occurrence={"Type": 2, "Count": 1},
    )

    return [
        FieldCase(
            name="correlated_window_missing_days_start_is_strict",
            cohort_json=base_expr(correlated=correlated),
            build_omop=build,
        )
    ]


def correlated_window_boundary_cases() -> list[FieldCase]:
    def base_expr(*, correlated: dict) -> dict:
        expr = _base_cohort_expression("ConditionOccurrence", {})
        primary = expr["PrimaryCriteria"]["CriteriaList"][0]["ConditionOccurrence"]
        primary["CorrelatedCriteria"] = {
            "Type": "ALL",
            "CriteriaList": [correlated],
            "DemographicCriteriaList": [],
            "Groups": [],
        }
        expr["ConceptSets"].extend([_concept_set(codeset_id=2, concept_id=2002)])
        return expr

    def build_inclusive(builder: OmopBuilder) -> None:
        for person_id, corr_start in [
            (1, date(2000, 5, 31)),  # exactly lower bound (inclusive)
            (2, date(2000, 5, 30)),  # outside
        ]:
            builder.add_person(person_id=person_id)
            builder.add_observation_period(person_id=person_id, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
            v = builder.add_visit_occurrence(
                person_id=person_id,
                visit_start_date=date(2000, 6, 1),
                visit_end_date=date(2000, 6, 1),
                visit_concept_id=0,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=1001,
                condition_start_date=date(2000, 6, 1),
                condition_end_date=date(2000, 6, 5),
                visit_occurrence_id=v,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=2002,
                condition_start_date=corr_start,
                visit_occurrence_id=v,
            )

    inclusive = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        start_window={
            "Start": {"Days": 1, "Coeff": -1},
            "End": {"Days": 0, "Coeff": 1},
            "UseIndexEnd": False,
            "UseEventEnd": False,
        },
        occurrence={"Type": 2, "Count": 1},
    )

    def build_use_index_end(builder: OmopBuilder) -> None:
        for person_id, corr_start in [
            (1, date(2000, 6, 1)),  # on index start (should NOT match)
            (2, date(2000, 6, 5)),  # on index end (should match)
        ]:
            builder.add_person(person_id=person_id)
            builder.add_observation_period(person_id=person_id, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
            v = builder.add_visit_occurrence(
                person_id=person_id,
                visit_start_date=date(2000, 6, 1),
                visit_end_date=date(2000, 6, 5),
                visit_concept_id=0,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=1001,
                condition_start_date=date(2000, 6, 1),
                condition_end_date=date(2000, 6, 5),
                visit_occurrence_id=v,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=2002,
                condition_start_date=corr_start,
                visit_occurrence_id=v,
            )

    use_index_end = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        start_window={
            "Start": {"Days": 0, "Coeff": 1},
            "End": {"Days": 0, "Coeff": 1},
            "UseIndexEnd": True,
            "UseEventEnd": False,
        },
        occurrence={"Type": 2, "Count": 1},
    )

    def build_use_event_end(builder: OmopBuilder) -> None:
        for person_id, corr_start, corr_end in [
            (1, date(2000, 5, 31), date(2000, 6, 1)),  # end hits anchor date
            (2, date(2000, 5, 30), date(2000, 5, 31)),  # end outside
        ]:
            builder.add_person(person_id=person_id)
            builder.add_observation_period(person_id=person_id, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
            v = builder.add_visit_occurrence(
                person_id=person_id,
                visit_start_date=date(2000, 6, 1),
                visit_end_date=date(2000, 6, 1),
                visit_concept_id=0,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=1001,
                condition_start_date=date(2000, 6, 1),
                condition_end_date=date(2000, 6, 1),
                visit_occurrence_id=v,
            )
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=2002,
                condition_start_date=corr_start,
                condition_end_date=corr_end,
                visit_occurrence_id=v,
            )

    use_event_end = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        start_window={
            "Start": {"Days": 0, "Coeff": 1},
            "End": {"Days": 0, "Coeff": 1},
            "UseIndexEnd": False,
            "UseEventEnd": True,
        },
        occurrence={"Type": 2, "Count": 1},
    )

    return [
        FieldCase(
            name="correlated_window_inclusive_lower_bound",
            cohort_json=base_expr(correlated=inclusive),
            build_omop=build_inclusive,
        ),
        FieldCase(
            name="correlated_window_use_index_end",
            cohort_json=base_expr(correlated=use_index_end),
            build_omop=build_use_index_end,
        ),
        FieldCase(
            name="correlated_window_use_event_end",
            cohort_json=base_expr(correlated=use_event_end),
            build_omop=build_use_event_end,
        ),
    ]


def primary_criteria_limit_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        for person_id, starts in [
            (1, [date(2000, 6, 1), date(2000, 6, 2)]),
            (2, [date(2000, 6, 1)]),
        ]:
            builder.add_person(person_id=person_id)
            builder.add_observation_period(
                person_id=person_id,
                start_date=date(2000, 1, 1),
                end_date=date(2001, 1, 1),
            )
            for d in starts:
                builder.add_condition_occurrence(
                    person_id=person_id,
                    condition_concept_id=1001,
                    condition_start_date=d,
                )

    def expr(limit: dict | None) -> dict:
        cohort = _base_cohort_expression("ConditionOccurrence", {})
        if limit is None:
            cohort["PrimaryCriteria"].pop("PrimaryCriteriaLimit", None)
        else:
            cohort["PrimaryCriteria"]["PrimaryCriteriaLimit"] = limit
        return cohort

    return [
        FieldCase(
            name="primary_criteria_limit_default_first",
            cohort_json=expr(None),
            build_omop=build,
        ),
        FieldCase(
            name="primary_criteria_limit_all",
            cohort_json=expr({"Type": "All"}),
            build_omop=build,
        ),
        FieldCase(
            name="primary_criteria_limit_first",
            cohort_json=expr({"Type": "First"}),
            build_omop=build,
        ),
    ]


def observation_window_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(
            person_id=1,
            start_date=date(2000, 1, 1),
            end_date=date(2000, 1, 10),
        )
        for d in [
            date(2000, 1, 1),  # before bound (prior=2)
            date(2000, 1, 3),  # inclusive lower bound
            date(2000, 1, 8),  # inclusive upper bound (post=2)
            date(2000, 1, 10),  # after bound
        ]:
            builder.add_condition_occurrence(
                person_id=1,
                condition_concept_id=1001,
                condition_start_date=d,
            )

    cohort = _base_cohort_expression("ConditionOccurrence", {})
    cohort["PrimaryCriteria"]["ObservationWindow"] = {"PriorDays": 2, "PostDays": 2}
    cohort["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
    return [FieldCase(name="primary_observation_window_filters_by_op_bounds", cohort_json=cohort, build_omop=build)]


def collapse_era_pad_cases() -> list[FieldCase]:
    def build(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(
            person_id=1,
            start_date=date(2000, 1, 1),
            end_date=date(2000, 2, 1),
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 1, 1),
            condition_end_date=date(2000, 1, 1),
        )
        builder.add_condition_occurrence(
            person_id=1,
            condition_concept_id=1001,
            condition_start_date=date(2000, 1, 3),
            condition_end_date=date(2000, 1, 3),
        )

    def expr(pad: int) -> dict:
        cohort = _base_cohort_expression("ConditionOccurrence", {})
        cohort["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
        cohort["CollapseSettings"] = {"CollapseType": "ERA", "EraPad": pad}
        return cohort

    return [
        FieldCase(
            name="collapse_era_pad_merges_events",
            cohort_json=expr(2),
            build_omop=build,
        ),
        FieldCase(
            name="collapse_era_pad_zero_keeps_separate",
            cohort_json=expr(0),
            build_omop=build,
        ),
    ]


def correlated_restrict_visit_ignore_observation_cases() -> list[FieldCase]:
    def base_expr(*, correlated: dict) -> dict:
        expr = _base_cohort_expression("ConditionOccurrence", {})
        primary = expr["PrimaryCriteria"]["CriteriaList"][0]["ConditionOccurrence"]
        primary["CorrelatedCriteria"] = {
            "Type": "ALL",
            "CriteriaList": [correlated],
            "DemographicCriteriaList": [],
            "Groups": [],
        }
        expr["ConceptSets"].extend([_concept_set(codeset_id=2, concept_id=2002)])
        expr["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
        return expr

    def build_restrict(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v1 = builder.add_visit_occurrence(person_id=1, visit_start_date=date(2000, 6, 1), visit_end_date=date(2000, 6, 1), visit_concept_id=0)
        v2 = builder.add_visit_occurrence(person_id=1, visit_start_date=date(2000, 6, 2), visit_end_date=date(2000, 6, 2), visit_concept_id=0)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v1)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=2002, condition_start_date=date(2000, 6, 2), visit_occurrence_id=v2)

        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(1999, 1, 1), end_date=date(2001, 1, 1))
        v3 = builder.add_visit_occurrence(person_id=2, visit_start_date=date(2000, 6, 1), visit_end_date=date(2000, 6, 1), visit_concept_id=0)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v3)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=2002, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v3)

    restrict_visit_true = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        occurrence={"Type": 2, "Count": 1},
        restrict_visit=True,
    )
    restrict_visit_false = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        occurrence={"Type": 2, "Count": 1},
        restrict_visit=False,
    )

    def build_ignore_op(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 6, 1), end_date=date(2000, 6, 1))
        v1 = builder.add_visit_occurrence(person_id=1, visit_start_date=date(2000, 6, 1), visit_end_date=date(2000, 6, 1), visit_concept_id=0)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v1)
        builder.add_condition_occurrence(person_id=1, condition_concept_id=2002, condition_start_date=date(2000, 6, 5), visit_occurrence_id=v1)

        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 6, 1), end_date=date(2000, 6, 10))
        v2 = builder.add_visit_occurrence(person_id=2, visit_start_date=date(2000, 6, 1), visit_end_date=date(2000, 6, 1), visit_concept_id=0)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 6, 1), visit_occurrence_id=v2)
        builder.add_condition_occurrence(person_id=2, condition_concept_id=2002, condition_start_date=date(2000, 6, 5), visit_occurrence_id=v2)

    ignore_op_false = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        occurrence={"Type": 2, "Count": 1},
        ignore_observation_period=False,
    )
    ignore_op_true = _correlated_criteria_item(
        criteria={"ConditionOccurrence": {"CodesetId": 2}},
        occurrence={"Type": 2, "Count": 1},
        ignore_observation_period=True,
    )

    return [
        FieldCase(
            name="correlated_restrict_visit_true_excludes_cross_visit",
            cohort_json=base_expr(correlated=restrict_visit_true),
            build_omop=build_restrict,
        ),
        FieldCase(
            name="correlated_restrict_visit_false_allows_cross_visit",
            cohort_json=base_expr(correlated=restrict_visit_false),
            build_omop=build_restrict,
        ),
        FieldCase(
            name="correlated_ignore_observation_period_false_excludes_outside_op",
            cohort_json=base_expr(correlated=ignore_op_false),
            build_omop=build_ignore_op,
        ),
        FieldCase(
            name="correlated_ignore_observation_period_true_includes_outside_op",
            cohort_json=base_expr(correlated=ignore_op_true),
            build_omop=build_ignore_op,
        ),
    ]


def date_range_extent_cases() -> list[FieldCase]:
    def build_start(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 15))
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 7, 1))

    def build_end_inclusive(builder: OmopBuilder) -> None:
        builder.add_person(person_id=1)
        builder.add_observation_period(person_id=1, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=1, condition_concept_id=1001, condition_start_date=date(2000, 6, 30))
        builder.add_person(person_id=2)
        builder.add_observation_period(person_id=2, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
        builder.add_condition_occurrence(person_id=2, condition_concept_id=1001, condition_start_date=date(2000, 7, 1))

    cohort = _base_cohort_expression(
        "ConditionOccurrence",
        {"OccurrenceStartDate": {"Value": "2000-06-01", "Op": "bt", "Extent": "2000-06-30"}},
    )
    cohort["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
    return [
        FieldCase(name="date_range_between_uses_extent", cohort_json=cohort, build_omop=build_start),
        FieldCase(name="date_range_between_end_inclusive", cohort_json=cohort, build_omop=build_end_inclusive),
    ]


def correlated_criteria_inherited_field_cases() -> list[FieldCase]:
    def _make_case(criteria_type: str, *, count: int) -> FieldCase:
        cohort = _base_cohort_expression(criteria_type, {})
        cohort["PrimaryCriteria"]["PrimaryCriteriaLimit"] = {"Type": "All"}
        cohort["ConceptSets"].extend([_concept_set(codeset_id=2, concept_id=2002)])
        primary = cohort["PrimaryCriteria"]["CriteriaList"][0][criteria_type]
        primary["CorrelatedCriteria"] = {
            "Type": "ALL",
            "CriteriaList": [
                _correlated_criteria_item(
                    criteria={"ConditionOccurrence": {"CodesetId": 2}},
                    occurrence={"Type": 2, "Count": count},
                )
            ],
            "DemographicCriteriaList": [],
            "Groups": [],
        }

        def build(builder: OmopBuilder) -> None:
            for person_id, corr_events in [(1, count), (2, max(count - 1, 0))]:
                builder.add_person(person_id=person_id)
                builder.add_observation_period(person_id=person_id, start_date=date(2000, 1, 1), end_date=date(2001, 1, 1))
                if criteria_type == "DrugEra":
                    builder.add_drug_era(person_id=person_id, drug_concept_id=1001, drug_era_start_date=date(2000, 6, 1), drug_era_end_date=date(2000, 6, 2))
                elif criteria_type == "DrugExposure":
                    builder.add_drug_exposure(person_id=person_id, drug_concept_id=1001, drug_exposure_start_date=date(2000, 6, 1))
                elif criteria_type == "Measurement":
                    builder.add_measurement(person_id=person_id, measurement_concept_id=1001, measurement_date=date(2000, 6, 1), value_as_number=1.0, range_low=0.0, range_high=2.0)
                elif criteria_type == "Observation":
                    builder.add_observation(person_id=person_id, observation_concept_id=1001, observation_date=date(2000, 6, 1))
                elif criteria_type == "ProcedureOccurrence":
                    builder.add_procedure_occurrence(person_id=person_id, procedure_concept_id=1001, procedure_date=date(2000, 6, 1))
                elif criteria_type == "Specimen":
                    builder.add_specimen(person_id=person_id, specimen_concept_id=1001, specimen_date=date(2000, 6, 1))
                elif criteria_type == "VisitOccurrence":
                    builder.add_visit_occurrence(person_id=person_id, visit_start_date=date(2000, 6, 1), visit_end_date=date(2000, 6, 1), visit_concept_id=1001)
                else:
                    raise ValueError(f"Unsupported criteria type: {criteria_type}")

                for i in range(corr_events):
                    builder.add_condition_occurrence(
                        person_id=person_id,
                        condition_concept_id=2002,
                        condition_start_date=date(2000, 6, 1 + i),
                    )

        return FieldCase(
            name=f"{criteria_type.lower()}_correlated_criteria_count_{count}_discriminator",
            cohort_json=cohort,
            build_omop=build,
        )

    cases: list[FieldCase] = []
    for criteria_type in [
        "DrugEra",
        "DrugExposure",
        "Measurement",
        "Observation",
        "ProcedureOccurrence",
        "Specimen",
        "VisitOccurrence",
    ]:
        cases.append(_make_case(criteria_type, count=1))
        cases.append(_make_case(criteria_type, count=2))
    return cases


def generated_cases() -> list[FieldCase]:
    cases: list[FieldCase] = []
    cases.extend(observation_value_as_string_cases())
    cases.extend(drug_exposure_stop_reason_cases())
    cases.extend(device_exposure_unique_device_id_cases())
    cases.extend(death_occurrence_start_date_cases())
    cases.extend(measurement_range_high_ratio_cases())
    cases.extend(measurement_range_high_cases())
    cases.extend(procedure_occurrence_procedure_source_concept_cases())
    cases.extend(visit_occurrence_visit_source_concept_cases())
    cases.extend(visit_detail_visit_detail_source_concept_cases())
    cases.extend(visit_detail_codeset_id_cases())
    cases.extend(condition_occurrence_condition_source_concept_cases())
    cases.extend(condition_occurrence_condition_status_cases())
    cases.extend(condition_occurrence_condition_type_exclude_cases())
    cases.extend(condition_era_occurrence_count_cases())
    cases.extend(numeric_range_extent_cases())
    cases.extend(criteria_group_count_demographic_age_gender_cases())
    cases.extend(drug_era_era_length_cases())
    cases.extend(dose_era_codeset_id_cases())
    cases.extend(condition_occurrence_first_age_gender_date_visit_cases())
    cases.extend(measurement_basic_field_cases())
    cases.extend(observation_basic_field_cases())
    cases.extend(drug_exposure_basic_field_cases())
    cases.extend(device_exposure_first_and_type_exclude_cases())
    cases.extend(procedure_occurrence_first_and_type_exclude_cases())
    cases.extend(specimen_codeset_and_type_exclude_cases())
    cases.extend(observation_period_user_defined_period_cases())
    cases.extend(visit_occurrence_provider_specialty_and_visit_type_exclude_cases())
    cases.extend(death_death_type_exclude_cases())
    cases.extend(occurrence_correlated_criteria_cases())
    cases.extend(correlated_window_missing_days_cases())
    cases.extend(correlated_window_boundary_cases())
    cases.extend(primary_criteria_limit_cases())
    cases.extend(observation_window_cases())
    cases.extend(collapse_era_pad_cases())
    cases.extend(correlated_restrict_visit_ignore_observation_cases())
    cases.extend(date_range_extent_cases())
    cases.extend(correlated_criteria_inherited_field_cases())
    return cases
