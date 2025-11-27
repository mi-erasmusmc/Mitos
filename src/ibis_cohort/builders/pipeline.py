from __future__ import annotations

import ibis

import polars as pl

from ibis_cohort.build_context import BuildContext
from ibis_cohort.builders.common import apply_observation_window, apply_end_strategy, collapse_events
from ibis_cohort.builders.registry import build_events
from ibis_cohort.cohort_expression import CohortExpression

from . import condition_occurrence  # noqa: F401
from . import condition_era  # noqa: F401
from . import drug_exposure  # noqa: F401
from . import drug_era  # noqa: F401
from . import dose_era  # noqa: F401
from . import visit_occurrence  # noqa: F401
from . import measurement  # noqa: F401
from . import observation  # noqa: F401
from . import observation_period  # noqa: F401
from . import device_exposure  # noqa: F401
from . import procedure_occurrence  # noqa: F401
from . import death  # noqa: F401
from . import specimen  # noqa: F401
from .groups import apply_criteria_group
from .post_processing import apply_inclusion_rules, apply_censoring, apply_censor_window

OUTPUT_SCHEMA = {
    "person_id": pl.Int64,
    "event_id": pl.Int64,
    "start_date": pl.Datetime,
    "end_date": pl.Datetime,
    "visit_occurrence_id": pl.Int64,
}


def build_primary_events(expression: CohortExpression, ctx: BuildContext):
    primary = expression.primary_criteria
    event_tables = [build_events(criteria, ctx) for criteria in primary.criteria_list]
    if not event_tables:
        return None
    events = event_tables[0]
    for table in event_tables[1:]:
        events = events.union(table, distinct=False)
    events = events.mutate(_source_event_id=events.event_id)
    events = apply_observation_window(events, primary.observation_window, ctx)
    events = _assign_primary_event_ids(events)
    events = apply_criteria_group(events, expression.additional_criteria, ctx)
    events = apply_inclusion_rules(events, expression.inclusion_rules, ctx)
    events = apply_censoring(events, expression.censoring_criteria, ctx)
    events = _apply_result_limit(events, expression.expression_limit)
    events = apply_end_strategy(events, expression.end_strategy, ctx)
    if primary.primary_limit and primary.primary_limit.type.lower() == "first":
        if "_person_ordinal" in events.columns:
            events = events.filter(events._person_ordinal == 1)
        else:
            window = ibis.window(group_by=events.person_id, order_by=[events.start_date, events.event_id])
            events = events.mutate(_row_number=ibis.row_number().over(window)).filter(lambda t: t._row_number == 0)
            if "_row_number" in events.columns:
                cols = [col for col in events.columns if col != "_row_number"]
                events = events.select(*cols)
    events = apply_censor_window(events, expression.censor_window, ctx)
    drop_cols = [
        col
        for col in (
            "_source_event_id",
            "_person_ordinal",
            "observation_period_start_date",
            "observation_period_end_date",
        )
        if col in events.columns
    ]
    if drop_cols:
        events = events.drop(*drop_cols)
    events = collapse_events(events, expression.collapse_settings)
    return events


def build_primary_events_polars(expression: CohortExpression, ctx: BuildContext) -> pl.DataFrame:
    events = build_primary_events(expression, ctx)
    if events is None:
        return pl.DataFrame(schema=OUTPUT_SCHEMA)
    return events.to_polars()


def _assign_primary_event_ids(events):
    if "_source_event_id" not in events.columns:
        events = events.mutate(_source_event_id=events.event_id)
    order = [events.person_id, events.start_date, events._source_event_id]
    global_window = ibis.window(order_by=order)
    person_window = ibis.window(group_by=events.person_id, order_by=order[1:])
    global_rank = ibis.row_number().over(global_window)
    person_rank = ibis.row_number().over(person_window)
    events = events.mutate(
        event_id=(global_rank + 1),
        _person_ordinal=(person_rank + 1),
    )
    supplemental = [
        events[column]
        for column in ("observation_period_start_date", "observation_period_end_date")
        if column in events.columns
    ]
    return events.select(
        events.person_id,
        events.event_id,
        events.start_date,
        events.end_date,
        events.visit_occurrence_id,
        events._source_event_id,
        events._person_ordinal,
        *supplemental,
    )


def _apply_result_limit(events, limit):
    if not limit or (limit.type or "ALL").lower() == "all":
        return events
    order_by = [events.start_date]
    if "event_id" in events.columns:
        order_by.append(events.event_id)
    window = ibis.window(group_by=events.person_id, order_by=order_by)
    ranked = events.mutate(_result_row=ibis.row_number().over(window))
    limited = ranked.filter(ranked._result_row == 0)
    return limited.drop("_result_row")
