from __future__ import annotations

from ibis_cohort.build_context import BuildContext
from ibis_cohort.tables import VisitDetail
from ibis_cohort.builders.common import (
    apply_codeset_filter,
    apply_date_range,
    apply_concept_set_selection,
    apply_interval_range,
    apply_age_filter,
    apply_gender_filter,
    apply_provider_specialty_filter,
    apply_care_site_filter,
    apply_location_region_filter,
    apply_first_event,
    standardize_output,
)
from ibis_cohort.builders.groups import apply_criteria_group
from ibis_cohort.builders.registry import register


@register("VisitDetail")
def build_visit_detail(criteria: VisitDetail, ctx: BuildContext):
    table = ctx.table("visit_detail")

    table = apply_codeset_filter(table, "visit_detail_concept_id", criteria.codeset_id, ctx)
    table = apply_date_range(table, "visit_detail_start_date", criteria.visit_detail_start_date)
    table = apply_date_range(table, "visit_detail_end_date", criteria.visit_detail_end_date)
    table = apply_concept_set_selection(table, "visit_detail_type_concept_id", criteria.visit_detail_type_cs, ctx)
    table = apply_codeset_filter(table, "visit_detail_source_concept_id", criteria.visit_detail_source_concept, ctx)
    table = apply_interval_range(table, "visit_detail_start_date", "visit_detail_end_date", criteria.visit_detail_length)

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, "visit_detail_end_date")
    table = apply_gender_filter(table, [], criteria.gender_cs, ctx)
    table = apply_provider_specialty_filter(table, criteria.provider_specialty_cs, ctx)
    table = apply_care_site_filter(table, criteria.place_of_service_cs, ctx)
    table = apply_location_region_filter(
        table,
        care_site_column="care_site_id",
        location_codeset_id=criteria.place_of_service_location,
        start_column="visit_detail_start_date",
        end_column="visit_detail_end_date",
        ctx=ctx,
    )

    if criteria.first:
        table = apply_first_event(table, "visit_detail_start_date", "visit_detail_id")

    events = standardize_output(
        table,
        primary_key="visit_detail_id",
        start_column="visit_detail_start_date",
        end_column="visit_detail_end_date",
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
