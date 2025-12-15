from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import ibis

from mitos.cohort_expression import CohortExpression
from mitos.testing.omop.builder import OmopBuilder
from mitos.testing.omop.vocab import build_minimal_vocab
from mitos.testing.omop.phenotype import (
    codeset_to_concept_id,
    generate_event_for_correlated_criteria,
)


PHENOTYPE_216_JSON = Path("cohorts/phenotype-216.json")


@dataclass(frozen=True)
class Phenotype216Expectations:
    include_person_ids: set[int]
    exclude_person_ids: set[int]
    washout_person_id: int
    censor_person_id: int
    censor_expected_end_date: date


def build_fake_omop_for_phenotype_216(
    con: ibis.BaseBackend,
    *,
    schema: str = "main",
) -> tuple[CohortExpression, Phenotype216Expectations]:
    """
    Build a minimal OMOP dataset designed to exercise phenotype-216 logic.

    CI assertions:
      - Person 1 passes all inclusion rules.
      - For each inclusion rule (except washout), one person fails it by having a matching event.
      - Washout rule is exercised by a person with two index events; only the first remains.
      - Censoring is exercised by a person with a censoring platelet measurement after index.
    """
    expression = CohortExpression.model_validate_json(PHENOTYPE_216_JSON.read_text())

    # Materialize minimal vocab so compile_codesets (and optional Circe SQL on DuckDB) can run.
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
    primary_concept = codeset_map[30]

    index_date = date(2020, 1, 15)
    op_start = date(2018, 1, 1)
    op_end = date(2022, 1, 1)

    builder = OmopBuilder(schema=schema)

    # Baseline passing person.
    passing_person = 1
    builder.add_person(person_id=passing_person)
    builder.add_observation_period(person_id=passing_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=passing_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )

    # Washout: person has 2 index events within 365 days; only the first should survive rule 1.
    washout_person = 2
    builder.add_person(person_id=washout_person)
    builder.add_observation_period(person_id=washout_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=washout_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    builder.add_condition_occurrence(
        person_id=washout_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date + timedelta(days=200),
        condition_end_date=index_date + timedelta(days=200),
    )

    # Censoring: trigger censoring platelet measurement (>150) after index date.
    censor_person = 3
    builder.add_person(person_id=censor_person)
    builder.add_observation_period(person_id=censor_person, start_date=op_start, end_date=op_end)
    builder.add_condition_occurrence(
        person_id=censor_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    censor_date = index_date + timedelta(days=30)
    platelet_codeset = 7
    platelet_concept = codeset_map[platelet_codeset]
    builder.add_measurement(
        person_id=censor_person,
        measurement_concept_id=platelet_concept,
        measurement_date=censor_date,
        value_as_number=200.0,
        unit_concept_id=8848,  # thousand per microliter (normalization multiplier=1)
    )

    # One person per inclusion rule (excluding washout rule 1), each failing exactly one rule.
    failing_people: list[int] = []
    for idx, rule in enumerate(expression.inclusion_rules or [], start=1):
        if idx == 1:
            continue
        person_id = 1000 + idx
        failing_people.append(person_id)
        builder.add_person(person_id=person_id)
        builder.add_observation_period(person_id=person_id, start_date=op_start, end_date=op_end)
        builder.add_condition_occurrence(
            person_id=person_id,
            condition_concept_id=primary_concept,
            condition_start_date=index_date,
            condition_end_date=index_date,
        )

        group = rule.expression
        correlated = group.criteria_list[0] if group and group.criteria_list else None
        ev = generate_event_for_correlated_criteria(
            correlated,
            index_date=index_date,
            codeset_map=codeset_map,
        )
        if ev is None:
            raise RuntimeError(f"Could not synthesize violating event for rule {idx}: {rule.name!r}")
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
        else:
            raise RuntimeError(f"Unsupported generated event kind: {ev.kind}")

    builder.materialize(con)

    expectations = Phenotype216Expectations(
        include_person_ids={passing_person, washout_person, censor_person},
        exclude_person_ids=set(failing_people),
        washout_person_id=washout_person,
        censor_person_id=censor_person,
        censor_expected_end_date=censor_date,
    )
    return expression, expectations

