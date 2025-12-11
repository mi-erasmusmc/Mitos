from __future__ import annotations

from mitos.build_context import BuildContext
from mitos.tables import ProcedureOccurrence
from mitos.builders.common import (
    apply_age_filter,
    apply_codeset_filter,
    apply_concept_filters,
    apply_concept_set_selection,
    apply_date_range,
    apply_first_event,
    apply_gender_filter,
    apply_numeric_range,
    apply_visit_concept_filters,
    standardize_output,
)
from mitos.builders.registry import register
from mitos.builders.groups import apply_criteria_group
from mitos.criteria import ConceptSetSelection


@register("ProcedureOccurrence")
def build_procedure_occurrence(criteria: ProcedureOccurrence, ctx: BuildContext):
    table = ctx.table("procedure_occurrence")

    concept_column = criteria.get_concept_id_column()
    table = apply_codeset_filter(table, concept_column, criteria.codeset_id, ctx)
    if criteria.first:
        table = apply_first_event(table, criteria.get_start_date_column(), criteria.get_primary_key_column())

    table = apply_date_range(table, criteria.get_start_date_column(), criteria.occurrence_start_date)
    table = apply_date_range(table, criteria.get_end_date_column(), criteria.occurrence_end_date)

    if criteria.procedure_type:
        table = apply_concept_filters(table, "procedure_type_concept_id", criteria.procedure_type)
    table = apply_concept_set_selection(table, "procedure_type_concept_id", criteria.procedure_type_cs, ctx)
    if criteria.procedure_type_exclude:
        table = apply_concept_filters(table, "procedure_type_concept_id", criteria.procedure_type, exclude=True)

    if criteria.modifier:
        table = apply_concept_filters(table, "modifier_concept_id", criteria.modifier)
    table = apply_concept_set_selection(table, "modifier_concept_id", criteria.modifier_cs, ctx)

    table = apply_numeric_range(table, "quantity", criteria.quantity)

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, criteria.get_start_date_column())
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)
    table = apply_visit_concept_filters(table, criteria.visit_type, criteria.visit_type_cs, ctx)

    source_filter = getattr(criteria, "procedure_source_concept", None)
    if source_filter is not None:
        if isinstance(source_filter, ConceptSetSelection):
            selection = source_filter
        else:
            selection = ConceptSetSelection(CodesetId=int(source_filter))
        table = apply_concept_set_selection(table, "procedure_source_concept_id", selection, ctx)

    events = standardize_output(
        table,
        primary_key=criteria.get_primary_key_column(),
        start_column=criteria.get_start_date_column(),
        end_column=criteria.get_end_date_column(),
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
