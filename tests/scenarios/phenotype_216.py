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
    washout_boundary_person_id: int
    washout_outside_person_id: int
    strategy_cap_person_id: int
    strategy_cap_expected_end_date: date
    censor_person_id: int
    censor_expected_end_date: date
    neutrophil_cells_unit_fail_person_id: int


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

    # Washout boundary: second event is exactly 365 days after the first.
    # If the washout lookback window is inclusive on the start boundary, the second event should be removed.
    washout_boundary_person = 4
    builder.add_person(person_id=washout_boundary_person)
    builder.add_observation_period(
        person_id=washout_boundary_person, start_date=op_start, end_date=op_end
    )
    builder.add_condition_occurrence(
        person_id=washout_boundary_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    builder.add_condition_occurrence(
        person_id=washout_boundary_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date + timedelta(days=365),
        condition_end_date=index_date + timedelta(days=365),
    )

    # Outside washout: second event is 366 days after the first and should survive.
    washout_outside_person = 5
    builder.add_person(person_id=washout_outside_person)
    builder.add_observation_period(
        person_id=washout_outside_person, start_date=op_start, end_date=op_end
    )
    builder.add_condition_occurrence(
        person_id=washout_outside_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    builder.add_condition_occurrence(
        person_id=washout_outside_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date + timedelta(days=366),
        condition_end_date=index_date + timedelta(days=366),
    )

    # End-strategy cap: date offset (180d) should be capped by observation_period_end_date.
    strategy_cap_person = 6
    strategy_cap_op_end = index_date + timedelta(days=60)
    builder.add_person(person_id=strategy_cap_person)
    builder.add_observation_period(
        person_id=strategy_cap_person, start_date=op_start, end_date=strategy_cap_op_end
    )
    builder.add_condition_occurrence(
        person_id=strategy_cap_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
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

    # Targeted regression for rule 16 ("No low neutrophil count within 7 days"):
    # create a measurement in *cells/uL* scale (unit 8784) that should violate the "no low neutrophil"
    # rule for the index event. This is exactly the kind of mixed-unit rule that broke parity.
    neutrophil_cells_unit_fail_person = 7
    builder.add_person(person_id=neutrophil_cells_unit_fail_person)
    builder.add_observation_period(
        person_id=neutrophil_cells_unit_fail_person, start_date=op_start, end_date=op_end
    )
    builder.add_condition_occurrence(
        person_id=neutrophil_cells_unit_fail_person,
        condition_concept_id=primary_concept,
        condition_start_date=index_date,
        condition_end_date=index_date,
    )
    # Find the rule by name and use its 2nd correlated criterion (cells/uL branch).
    rule16 = None
    for rule in expression.inclusion_rules or []:
        if (rule.name or "").strip().lower().startswith("no low neutrophil count within 7 days"):
            rule16 = rule
            break
    if rule16 is None or not getattr(rule16.expression, "criteria_list", None):
        raise RuntimeError("Phenotype-216: could not locate rule 16 (neutrophil) criteria list")
    if len(rule16.expression.criteria_list) < 2:
        raise RuntimeError("Phenotype-216: expected rule 16 to have multiple measurement criteria branches")
    neutrophil_branch = rule16.expression.criteria_list[1]
    ev = generate_event_for_correlated_criteria(
        neutrophil_branch,
        index_date=index_date,
        codeset_map=codeset_map,
    )
    if ev is None or ev.kind != "measurement":
        raise RuntimeError("Phenotype-216: failed to synthesize neutrophil cells/uL measurement event")
    builder.add_measurement(
        person_id=neutrophil_cells_unit_fail_person,
        measurement_concept_id=ev.payload["measurement_concept_id"],
        measurement_date=ev.payload["measurement_date"],
        value_as_number=ev.payload.get("value_as_number"),
        unit_concept_id=ev.payload.get("unit_concept_id", 0),
        range_low=ev.payload.get("range_low"),
        range_high=ev.payload.get("range_high"),
    )

    # One person per inclusion rule branch (excluding washout rule 1).
    # If a rule has multiple correlated criteria entries (e.g. mixed-unit measurement branches),
    # we create one failing person per entry to ensure each branch is exercised.
    failing_people: list[int] = [neutrophil_cells_unit_fail_person]
    next_fail_id = 1000
    for idx, rule in enumerate(expression.inclusion_rules or [], start=1):
        if idx == 1:
            continue

        group = rule.expression
        correlated_list = list(getattr(group, "criteria_list", []) or [])
        if not correlated_list:
            raise RuntimeError(f"Phenotype-216: rule {idx} has no correlated criteria: {rule.name!r}")

        for correlated in correlated_list:
            person_id = next_fail_id
            next_fail_id += 1
            failing_people.append(person_id)
            builder.add_person(person_id=person_id)
            builder.add_observation_period(person_id=person_id, start_date=op_start, end_date=op_end)
            builder.add_condition_occurrence(
                person_id=person_id,
                condition_concept_id=primary_concept,
                condition_start_date=index_date,
                condition_end_date=index_date,
            )

            ev = generate_event_for_correlated_criteria(
                correlated,
                index_date=index_date,
                codeset_map=codeset_map,
            )
            if ev is None:
                raise RuntimeError(
                    f"Could not synthesize violating event for rule {idx}: {rule.name!r}"
                )
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
            else:
                raise RuntimeError(f"Unsupported generated event kind: {ev.kind}")

    builder.materialize(con)

    expectations = Phenotype216Expectations(
        include_person_ids={
            passing_person,
            washout_person,
            washout_boundary_person,
            washout_outside_person,
            strategy_cap_person,
            censor_person,
        },
        exclude_person_ids=set(failing_people),
        washout_person_id=washout_person,
        washout_boundary_person_id=washout_boundary_person,
        washout_outside_person_id=washout_outside_person,
        strategy_cap_person_id=strategy_cap_person,
        strategy_cap_expected_end_date=strategy_cap_op_end,
        censor_person_id=censor_person,
        censor_expected_end_date=censor_date,
        neutrophil_cells_unit_fail_person_id=neutrophil_cells_unit_fail_person,
    )
    return expression, expectations
