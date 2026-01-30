from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from mitos.cohort_expression import CohortExpression
from mitos.tables import (
    parse_single_criteria,
    ConditionOccurrence,
    Measurement,
    Observation,
    VisitOccurrence,
    Death,
)


@dataclass(frozen=True)
class GeneratedEvent:
    kind: str  # "condition_occurrence" | "measurement" | "observation" | "visit_occurrence" | "death"
    payload: dict[str, Any]


def codeset_to_concept_id(expression: CohortExpression) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for cs in expression.concept_sets or []:
        cs_id = int(cs.id)
        expr = cs.expression
        items = expr.items if expr and expr.items else []
        chosen = None
        for item in items:
            if item.is_excluded:
                continue
            concept = item.concept
            if concept is None or concept.concept_id is None:
                continue
            chosen = int(concept.concept_id)
            break
        if chosen is not None:
            mapping[cs_id] = chosen
    return mapping


def _compute_bound(anchor: date, endpoint) -> date | None:
    if endpoint is None or endpoint.days is None:
        return None
    coeff = int(endpoint.coeff if endpoint.coeff is not None else 1)
    return anchor + timedelta(days=int(endpoint.days) * coeff)


def choose_date_in_start_window(index_date: date, start_window) -> date:
    if start_window is None:
        return index_date
    lower = _compute_bound(index_date, getattr(start_window, "start", None))
    upper = _compute_bound(index_date, getattr(start_window, "end", None))
    if lower is None and upper is None:
        return index_date
    if lower is None:
        return upper or index_date
    if upper is None:
        return lower
    if lower > upper:
        lower, upper = upper, lower
    return lower if lower != upper else lower


def pick_value_for_numeric_range(nr) -> float | None:
    if nr is None or nr.value is None:
        return None
    op = (nr.op or "eq").lower()
    value = float(nr.value)
    extent = float(nr.extent) if nr.extent is not None else None
    if op.endswith("bt") and extent is not None:
        return value if value <= extent else extent
    if op in ("gt",):
        return value + 1.0
    if op in ("gte",):
        return value
    if op in ("lt",):
        return value - 1.0
    if op in ("lte",):
        return value
    if op in ("eq",):
        return value
    if op in ("!eq", "neq"):
        return value + 1.0
    return value


def generate_event_for_correlated_criteria(
    correlated,
    *,
    index_date: date,
    codeset_map: dict[int, int],
) -> GeneratedEvent | None:
    if correlated is None or correlated.criteria is None:
        return None
    criteria_model = parse_single_criteria(correlated.criteria)

    event_date = choose_date_in_start_window(index_date, correlated.start_window)

    if isinstance(criteria_model, VisitOccurrence):
        codeset_id = (
            int(criteria_model.codeset_id)
            if criteria_model.codeset_id is not None
            else None
        )
        if codeset_id is None or codeset_id not in codeset_map:
            return None
        concept_id = codeset_map[codeset_id]

        correlated_events: list[GeneratedEvent] = []
        group = getattr(criteria_model, "correlated_criteria", None)
        if group and getattr(group, "criteria_list", None):
            for nested in group.criteria_list or []:
                nested_ev = generate_event_for_correlated_criteria(
                    nested,
                    index_date=event_date,
                    codeset_map=codeset_map,
                )
                if nested_ev is not None:
                    correlated_events.append(nested_ev)

        return GeneratedEvent(
            kind="visit_occurrence",
            payload={
                "visit_concept_id": concept_id,
                "visit_start_date": event_date,
                "visit_end_date": event_date + timedelta(days=1),
                "correlated_events": correlated_events,
            },
        )

    if isinstance(criteria_model, ConditionOccurrence):
        codeset_id = (
            int(criteria_model.codeset_id)
            if criteria_model.codeset_id is not None
            else None
        )
        if codeset_id is None or codeset_id not in codeset_map:
            return None
        concept_id = codeset_map[codeset_id]
        return GeneratedEvent(
            kind="condition_occurrence",
            payload={
                "condition_concept_id": concept_id,
                "condition_start_date": event_date,
                "condition_end_date": event_date,
            },
        )

    if isinstance(criteria_model, Measurement):
        codeset_id = (
            int(criteria_model.codeset_id)
            if criteria_model.codeset_id is not None
            else None
        )
        if codeset_id is None or codeset_id not in codeset_map:
            return None
        concept_id = codeset_map[codeset_id]
        unit_id = 0
        if criteria_model.unit:
            for u in criteria_model.unit:
                if u.concept_id is not None:
                    unit_id = int(u.concept_id)
                    break
        value = pick_value_for_numeric_range(criteria_model.value_as_number)
        range_low = pick_value_for_numeric_range(criteria_model.range_low)
        range_high = pick_value_for_numeric_range(criteria_model.range_high)
        # Ensure measurement numeric columns are present for filters that rely on them.
        return GeneratedEvent(
            kind="measurement",
            payload={
                "measurement_concept_id": concept_id,
                "measurement_date": event_date,
                "value_as_number": value,
                "unit_concept_id": unit_id,
                "range_low": range_low,
                "range_high": range_high,
            },
        )

    if isinstance(criteria_model, Observation):
        codeset_id = (
            int(criteria_model.codeset_id)
            if criteria_model.codeset_id is not None
            else None
        )
        if codeset_id is None or codeset_id not in codeset_map:
            return None
        concept_id = codeset_map[codeset_id]
        unit_id = 0
        if getattr(criteria_model, "unit", None):
            for u in criteria_model.unit:
                if u.concept_id is not None:
                    unit_id = int(u.concept_id)
                    break
        value = pick_value_for_numeric_range(
            getattr(criteria_model, "value_as_number", None)
        )
        return GeneratedEvent(
            kind="observation",
            payload={
                "observation_concept_id": concept_id,
                "observation_date": event_date,
                "value_as_number": value,
                "unit_concept_id": unit_id,
            },
        )

    if isinstance(criteria_model, Death):
        codeset_id = (
            int(criteria_model.codeset_id)
            if criteria_model.codeset_id is not None
            else None
        )
        if codeset_id is None or codeset_id not in codeset_map:
            return None
        concept_id = codeset_map[codeset_id]
        return GeneratedEvent(
            kind="death",
            payload={
                "death_date": event_date,
                "cause_concept_id": concept_id,
            },
        )

    return None
