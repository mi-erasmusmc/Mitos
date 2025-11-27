from __future__ import annotations

import ibis
import ibis.expr.types as ir

from ibis_cohort.build_context import BuildContext
from ibis_cohort.builders.groups import apply_criteria_group
from ibis_cohort.builders.registry import build_events
from ibis_cohort.cohort_expression import InclusionRule
from ibis_cohort.tables import Criteria, DrugExposure
from ibis_cohort.builders.common import collapse_events


def apply_additional_criteria(events: ir.Table, group, ctx: BuildContext) -> ir.Table:
    return apply_criteria_group(events, group, ctx)


def apply_inclusion_rules(events: ir.Table, rules: list[InclusionRule], ctx: BuildContext) -> ir.Table:
    for rule in rules or []:
        events = apply_criteria_group(events, rule.expression, ctx)
    return events


def apply_censoring(events: ir.Table, criteria_list: list[Criteria], ctx: BuildContext) -> ir.Table:
    if not criteria_list:
        return events
    censor_tables = [build_events(criteria, ctx) for criteria in criteria_list if criteria]
    if not censor_tables:
        return events
    censor_events = censor_tables[0]
    for table in censor_tables[1:]:
        censor_events = censor_events.union(table)

    censor_events = censor_events.select(
        censor_events.person_id,
        censor_events.start_date.name("censor_start"),
    )
    joined = events.join(
        censor_events,
        (events.person_id == censor_events.person_id) & (censor_events.censor_start >= events.start_date),
        how="left",
    )
    min_censor = joined.group_by(events.event_id).aggregate(censor_date=censor_events.censor_start.min())
    events = events.left_join(min_censor, events.event_id == min_censor.event_id)
    events = events.mutate(
        end_date=ibis.ifelse(
            events.censor_date.notnull() & (events.censor_date < events.end_date),
            events.censor_date,
            events.end_date,
        )
    ).drop("censor_date")
    return events


def apply_censor_window(events: ir.Table, window, ctx: BuildContext) -> ir.Table:
    if not window:
        return events
    if window.start_date:
        events = events.filter(events.start_date >= ibis.timestamp(window.start_date))
    if window.end_date:
        events = events.filter(events.end_date <= ibis.timestamp(window.end_date))
    return events
