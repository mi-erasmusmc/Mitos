from __future__ import annotations

from mitos.build_context import BuildContext
from mitos.tables import DrugExposure
from mitos.builders.common import (
    apply_age_filter,
    apply_codeset_filter,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_date_range,
    apply_first_event,
    apply_gender_filter,
    apply_numeric_range,
    apply_provider_specialty_filter,
    apply_text_filter,
    apply_visit_concept_filters,
    standardize_output,
)
from mitos.builders.registry import register
from mitos.builders.groups import apply_criteria_group
from mitos.criteria import ConceptSetSelection


@register("DrugExposure")
def build_drug_exposure(criteria: DrugExposure, ctx: BuildContext):
    table = ctx.table("drug_exposure")

    concept_column = criteria.get_concept_id_column()
    table = apply_codeset_filter(table, concept_column, criteria.codeset_id, ctx)
    if criteria.first:
        table = apply_first_event(
            table, criteria.get_start_date_column(), criteria.get_primary_key_column()
        )

    table = apply_date_range(
        table, criteria.get_start_date_column(), criteria.occurrence_start_date
    )
    table = apply_date_range(
        table, criteria.get_end_date_column(), criteria.occurrence_end_date
    )

    table = apply_concept_filters(
        table,
        "drug_type_concept_id",
        criteria.drug_type,
        exclude=bool(getattr(criteria, "drug_type_exclude", False)),
    )
    table = apply_concept_set_selection(
        table, "drug_type_concept_id", criteria.drug_type_cs, ctx
    )
    table = apply_concept_filters(table, "route_concept_id", criteria.route_concept)
    table = apply_concept_set_selection(
        table, "route_concept_id", criteria.route_concept_cs, ctx
    )
    table = apply_concept_filters(
        table, "dose_unit_concept_id", getattr(criteria, "dose_unit", [])
    )
    table = apply_concept_set_selection(
        table, "dose_unit_concept_id", getattr(criteria, "dose_unit_cs", None), ctx
    )

    table = apply_numeric_range(table, "quantity", criteria.quantity)
    table = apply_numeric_range(table, "days_supply", criteria.days_supply)
    table = apply_numeric_range(table, "refills", criteria.refills)
    table = apply_text_filter(
        table, "stop_reason", getattr(criteria, "stop_reason", None)
    )
    table = apply_text_filter(
        table, "lot_number", getattr(criteria, "lot_number", None)
    )

    if criteria.age:
        table = apply_age_filter(
            table, criteria.age, ctx, criteria.get_start_date_column()
        )
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)
    table = apply_provider_specialty_filter(
        table,
        getattr(criteria, "provider_specialty", None),
        getattr(criteria, "provider_specialty_cs", None),
        ctx,
        provider_column="provider_id",
    )
    table = apply_visit_concept_filters(
        table, criteria.visit_type, criteria.visit_type_cs, ctx
    )

    source_filter = getattr(criteria, "drug_source_concept", None)
    if source_filter is not None:
        if isinstance(source_filter, ConceptSetSelection):
            selection = source_filter
        else:
            selection = ConceptSetSelection(CodesetId=int(source_filter))
        table = apply_concept_set_selection(
            table, "drug_source_concept_id", selection, ctx
        )

    events = standardize_output(
        table,
        primary_key=criteria.get_primary_key_column(),
        start_column=criteria.get_start_date_column(),
        end_column=criteria.get_end_date_column(),
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
