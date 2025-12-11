from __future__ import annotations

from mitos.build_context import BuildContext
from mitos.tables import Specimen
from mitos.builders.common import (
    apply_codeset_filter,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_date_range,
    apply_numeric_range,
    apply_text_filter,
    apply_age_filter,
    apply_gender_filter,
    apply_first_event,
    standardize_output,
)
from mitos.builders.groups import apply_criteria_group
from mitos.builders.registry import register


@register("Specimen")
def build_specimen(criteria: Specimen, ctx: BuildContext):
    table = ctx.table("specimen")

    table = apply_codeset_filter(table, "specimen_concept_id", criteria.codeset_id, ctx)
    table = apply_date_range(table, "specimen_date", criteria.occurrence_start_date)

    if criteria.specimen_type:
        table = apply_concept_filters(
            table,
            "specimen_type_concept_id",
            criteria.specimen_type,
            exclude=bool(criteria.specimen_type_exclude),
        )
    table = apply_concept_set_selection(table, "specimen_type_concept_id", criteria.specimen_type_cs, ctx)

    table = apply_numeric_range(table, "quantity", criteria.quantity)

    if criteria.unit:
        table = apply_concept_filters(table, "unit_concept_id", criteria.unit)
    table = apply_concept_set_selection(table, "unit_concept_id", criteria.unit_cs, ctx)

    if criteria.anatomic_site:
        table = apply_concept_filters(table, "anatomic_site_concept_id", criteria.anatomic_site)
    table = apply_concept_set_selection(table, "anatomic_site_concept_id", criteria.anatomic_site_cs, ctx)

    if criteria.disease_status:
        table = apply_concept_filters(table, "disease_status_concept_id", criteria.disease_status)
    table = apply_concept_set_selection(table, "disease_status_concept_id", criteria.disease_status_cs, ctx)

    table = apply_text_filter(table, "specimen_source_id", criteria.source_id)
    if criteria.specimen_source_concept is not None:
        table = table.filter(table.specimen_source_concept_id == int(criteria.specimen_source_concept))

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, "specimen_date")
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)

    if criteria.first:
        table = apply_first_event(table, "specimen_date", "specimen_id")

    events = standardize_output(
        table,
        primary_key="specimen_id",
        start_column="specimen_date",
        end_column="specimen_date",
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
