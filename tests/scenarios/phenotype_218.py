from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import ibis

from mitos.cohort_expression import CohortExpression
from mitos.testing.omop.builder import OmopBuilder
from mitos.testing.omop.vocab import build_minimal_vocab
from mitos.testing.omop.phenotype import codeset_to_concept_id, generate_event_for_correlated_criteria


PHENOTYPE_218_JSON = Path("cohorts/phenotype-218.json")


@dataclass(frozen=True)
class Phenotype218Expectations:
    include_person_ids: set[int]
    exclude_person_ids: set[int]
    washout_fail_person_id: int
    no_hosp_fail_person_id: int


def build_fake_omop_for_phenotype_218(
    con: ibis.BaseBackend,
    *,
    schema: str = "main",
) -> tuple[CohortExpression, Phenotype218Expectations]:
    """
    Build a minimal OMOP dataset designed to exercise phenotype-218 logic.

    CI assertions:
      - One person has an index event plus an inpatient visit and passes all rules.
      - One person violates the washout rule by having a prior qualifying event.
      - One person violates the hospitalization rule by missing the inpatient visit.
      - Each remaining exclusion rule is violated by adding a matching event in its lookback window.
    """
    expression = CohortExpression.model_validate_json(PHENOTYPE_218_JSON.read_text())

    vocab = build_minimal_vocab(expression.concept_sets or [])
    con.create_table("concept", obj=vocab.concept, database=schema, overwrite=True)
    con.create_table("concept_ancestor", obj=vocab.concept_ancestor, database=schema, overwrite=True)
    con.create_table(
        "concept_relationship",
        obj=vocab.concept_relationship,
        database=schema,
        overwrite=True,
    )

    codeset_map = codeset_to_concept_id(expression)

    index_date = date(2020, 1, 15)
    op_start = date(2018, 1, 1)
    op_end = date(2022, 1, 1)

    # Primary criteria uses codeset 1 (ConditionOccurrence or Observation); choose ConditionOccurrence.
    primary_concept = codeset_map[1]

    # Hospitalization visit criteria uses codeset 3.
    inpatient_visit_concept = codeset_map[3]

    builder = OmopBuilder(schema=schema)

    passing_person = 1
    builder.add_person(person_id=passing_person)
    builder.add_observation_period(person_id=passing_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=passing_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    builder.add_visit_occurrence(
        person_id=passing_person,
        visit_concept_id=inpatient_visit_concept,
        visit_start_date=index_date,
        visit_end_date=index_date + timedelta(days=1),
    )

    # Washout rule (180 days): violate by adding a prior qualifying event in the lookback window.
    washout_fail_person = 2
    builder.add_person(person_id=washout_fail_person)
    builder.add_observation_period(person_id=washout_fail_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=washout_fail_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    builder.add_visit_occurrence(
        person_id=washout_fail_person,
        visit_concept_id=inpatient_visit_concept,
        visit_start_date=index_date,
        visit_end_date=index_date + timedelta(days=1),
    )
    builder.add_condition_occurrence(
        person_id=washout_fail_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date - timedelta(days=30),
        condition_end_date=index_date - timedelta(days=30),
    )

    # Hospitalization required: violate by omitting the inpatient visit.
    no_hosp_fail_person = 3
    builder.add_person(person_id=no_hosp_fail_person)
    builder.add_observation_period(person_id=no_hosp_fail_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=no_hosp_fail_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )

    exclude_people: set[int] = {washout_fail_person, no_hosp_fail_person}

    # For each remaining rule, violate it by adding a matching event in its window.
    # Rules with occurrence=0 are "no X" rules; adding a matching event should exclude the person.
    next_fail_id = 1000
    for idx, rule in enumerate(expression.inclusion_rules or [], start=1):
        if idx in (1, 2):
            continue

        for correlated in rule.expression.criteria_list or []:
            if correlated.occurrence and int(correlated.occurrence.type) == 0:
                person_id = next_fail_id
                next_fail_id += 1
                exclude_people.add(person_id)
                builder.add_person(person_id=person_id)
                builder.add_observation_period(
                    person_id=person_id, start_date=op_start, end_date=op_end
                )
                builder.add_condition_occurrence(
                    person_id=person_id,
                    condition_concept_id=primary_concept,
                    condition_start_date=index_date,
                    condition_end_date=index_date,
                )
                builder.add_visit_occurrence(
                    person_id=person_id,
                    visit_concept_id=inpatient_visit_concept,
                    visit_start_date=index_date,
                    visit_end_date=index_date + timedelta(days=1),
                )

                ev = generate_event_for_correlated_criteria(
                    correlated,
                    index_date=index_date,
                    codeset_map=codeset_map,
                )
                if ev is None:
                    raise RuntimeError(f"Phenotype-218: failed to synthesize violating event for {rule.name!r}")

                if ev.kind == "condition_occurrence":
                    builder.add_condition_occurrence(
                        person_id=person_id,
                        condition_concept_id=ev.payload["condition_concept_id"],
                        condition_start_date=ev.payload["condition_start_date"],
                        condition_end_date=ev.payload["condition_end_date"],
                    )
                elif ev.kind == "measurement":
                    builder.add_measurement(
                        person_id=person_id,
                        measurement_concept_id=ev.payload["measurement_concept_id"],
                        measurement_date=ev.payload["measurement_date"],
                        value_as_number=ev.payload.get("value_as_number"),
                        unit_concept_id=ev.payload.get("unit_concept_id", 0),
                        range_low=ev.payload.get("range_low"),
                        range_high=ev.payload.get("range_high"),
                    )
                elif ev.kind == "observation":
                    builder.add_observation(
                        person_id=person_id,
                        observation_concept_id=ev.payload["observation_concept_id"],
                        observation_date=ev.payload["observation_date"],
                        value_as_number=ev.payload.get("value_as_number"),
                        unit_concept_id=ev.payload.get("unit_concept_id", 0),
                    )
                elif ev.kind == "visit_occurrence":
                    visit_occurrence_id = builder.add_visit_occurrence(
                        person_id=person_id,
                        visit_concept_id=ev.payload["visit_concept_id"],
                        visit_start_date=ev.payload["visit_start_date"],
                        visit_end_date=ev.payload["visit_end_date"],
                    )
                    for nested in ev.payload.get("correlated_events", []) or []:
                        if nested.kind == "condition_occurrence":
                            builder.add_condition_occurrence(
                                person_id=person_id,
                                condition_concept_id=nested.payload["condition_concept_id"],
                                condition_start_date=nested.payload["condition_start_date"],
                                condition_end_date=nested.payload["condition_end_date"],
                                visit_occurrence_id=visit_occurrence_id,
                            )
                        elif nested.kind == "measurement":
                            builder.add_measurement(
                                person_id=person_id,
                                measurement_concept_id=nested.payload["measurement_concept_id"],
                                measurement_date=nested.payload["measurement_date"],
                                value_as_number=nested.payload.get("value_as_number"),
                                unit_concept_id=nested.payload.get("unit_concept_id", 0),
                                range_low=nested.payload.get("range_low"),
                                range_high=nested.payload.get("range_high"),
                                visit_occurrence_id=visit_occurrence_id,
                            )
                        elif nested.kind == "observation":
                            builder.add_observation(
                                person_id=person_id,
                                observation_concept_id=nested.payload["observation_concept_id"],
                                observation_date=nested.payload["observation_date"],
                                value_as_number=nested.payload.get("value_as_number"),
                                unit_concept_id=nested.payload.get("unit_concept_id", 0),
                                visit_occurrence_id=visit_occurrence_id,
                            )
                        else:
                            raise RuntimeError(
                                f"Phenotype-218: unsupported nested event kind {nested.kind!r}"
                            )
                else:
                    raise RuntimeError(f"Phenotype-218: unsupported event kind {ev.kind!r}")

    builder.materialize(con)

    expectations = Phenotype218Expectations(
        include_person_ids={passing_person},
        exclude_person_ids=exclude_people,
        washout_fail_person_id=washout_fail_person,
        no_hosp_fail_person_id=no_hosp_fail_person,
    )
    return expression, expectations

