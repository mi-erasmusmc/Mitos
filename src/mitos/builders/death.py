from __future__ import annotations

import ibis

from mitos.build_context import BuildContext
from mitos.tables import Death
from mitos.builders.common import (
    apply_age_filter,
    apply_codeset_filter,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_date_range,
    apply_gender_filter,
    standardize_output,
)
from mitos.builders.registry import register
from mitos.builders.groups import apply_criteria_group


@register("Death")
def build_death(criteria: Death, ctx: BuildContext):
    table = ctx.table("death")

    table = apply_codeset_filter(table, "cause_concept_id", criteria.codeset_id, ctx)

    table = apply_date_range(table, "death_date", getattr(criteria, "occurrence_start_date", None))

    if criteria.death_type:
        table = apply_concept_filters(
            table,
            "death_type_concept_id",
            criteria.death_type,
            exclude=bool(getattr(criteria, "death_type_exclude", False)),
        )
    table = apply_concept_set_selection(table, "death_type_concept_id", criteria.death_type_cs, ctx)

    if getattr(criteria, "death_source_concept", None) is not None:
        table = apply_codeset_filter(
            table,
            "cause_source_concept_id",
            int(criteria.death_source_concept),
            ctx,
        )

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, criteria.get_start_date_column())
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)

    window = ibis.window(order_by=[table.person_id, table.death_date])
    table = table.mutate(death_event_id=ibis.row_number().over(window))

    events = standardize_output(
        table,
        primary_key="death_event_id",
        start_column="death_date",
        end_column="death_date",
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
