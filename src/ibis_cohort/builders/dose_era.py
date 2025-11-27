from __future__ import annotations

from ibis_cohort.build_context import BuildContext
from ibis_cohort.tables import DoseEra
from ibis_cohort.builders.common import (
    apply_codeset_filter,
    apply_date_range,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_numeric_range,
    apply_interval_range,
    apply_age_filter,
    apply_gender_filter,
    apply_first_event,
    standardize_output,
)
from ibis_cohort.builders.groups import apply_criteria_group
from ibis_cohort.builders.registry import register


@register("DoseEra")
def build_dose_era(criteria: DoseEra, ctx: BuildContext):
    table = ctx.table("dose_era")

    table = apply_codeset_filter(table, "drug_concept_id", criteria.codeset_id, ctx)
    table = apply_date_range(table, "dose_era_start_date", criteria.era_start_date)
    table = apply_date_range(table, "dose_era_end_date", criteria.era_end_date)

    if criteria.unit:
        table = apply_concept_filters(table, "unit_concept_id", criteria.unit)
    table = apply_concept_set_selection(table, "unit_concept_id", criteria.unit_cs, ctx)

    table = apply_numeric_range(table, "dose_value", criteria.dose_value)
    table = apply_interval_range(table, "dose_era_start_date", "dose_era_end_date", criteria.era_length)

    if criteria.age_at_start:
        table = apply_age_filter(table, criteria.age_at_start, ctx, "dose_era_start_date")
    if criteria.age_at_end:
        table = apply_age_filter(table, criteria.age_at_end, ctx, "dose_era_end_date")
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)

    if criteria.first:
        table = apply_first_event(table, "dose_era_start_date", "dose_era_id")

    events = standardize_output(
        table,
        primary_key="dose_era_id",
        start_column="dose_era_start_date",
        end_column="dose_era_end_date",
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
