from __future__ import annotations

from mitos.build_context import BuildContext
from mitos.tables import ObservationPeriod
from mitos.builders.common import (
    apply_date_range,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_interval_range,
    apply_age_filter,
    apply_first_event,
    apply_user_defined_period,
    standardize_output,
)
from mitos.builders.groups import apply_criteria_group
from mitos.builders.registry import register


@register("ObservationPeriod")
def build_observation_period(criteria: ObservationPeriod, ctx: BuildContext):
    table = ctx.table("observation_period")

    table = apply_date_range(
        table, "observation_period_start_date", criteria.period_start_date
    )
    table = apply_date_range(
        table, "observation_period_end_date", criteria.period_end_date
    )

    if criteria.period_type:
        table = apply_concept_filters(
            table, "period_type_concept_id", criteria.period_type
        )
    table = apply_concept_set_selection(
        table, "period_type_concept_id", criteria.period_type_cs, ctx
    )

    table = apply_interval_range(
        table,
        "observation_period_start_date",
        "observation_period_end_date",
        criteria.period_length,
    )

    if criteria.age_at_start:
        table = apply_age_filter(
            table, criteria.age_at_start, ctx, "observation_period_start_date"
        )
    if criteria.age_at_end:
        table = apply_age_filter(
            table, criteria.age_at_end, ctx, "observation_period_end_date"
        )

    table, start_column, end_column = apply_user_defined_period(
        table,
        "observation_period_start_date",
        "observation_period_end_date",
        criteria.user_defined_period,
    )

    if criteria.first:
        table = apply_first_event(table, start_column, "observation_period_id")

    events = standardize_output(
        table,
        primary_key="observation_period_id",
        start_column=start_column,
        end_column=end_column,
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
