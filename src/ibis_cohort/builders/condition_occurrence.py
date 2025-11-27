from __future__ import annotations

import ibis

from ibis_cohort.build_context import BuildContext
from ibis_cohort.tables import ConditionOccurrence
from ibis_cohort.builders.common import (
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
from ibis_cohort.builders.registry import register
from ibis_cohort.criteria import ConceptSetSelection
from ibis_cohort.builders.groups import apply_criteria_group


@register("ConditionOccurrence")
def build_condition_occurrence(criteria: ConditionOccurrence, ctx: BuildContext):
    table = ctx.table("condition_occurrence")

    concept_column = criteria.get_concept_id_column()
    table = apply_codeset_filter(table, concept_column, criteria.codeset_id, ctx)

    table = apply_date_range(table, criteria.get_start_date_column(), criteria.occurrence_start_date)
    table = apply_date_range(table, criteria.get_end_date_column(), criteria.occurrence_end_date)

    if criteria.condition_type:
        table = apply_concept_filters(table, "condition_type_concept_id", criteria.condition_type)
    table = apply_concept_set_selection(table, "condition_type_concept_id", criteria.condition_type_cs, ctx)

    if criteria.condition_type_exclude:
        table = apply_concept_filters(table, "condition_type_concept_id", criteria.condition_type or [], exclude=True)

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, criteria.get_start_date_column())
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)

    source_filter = getattr(criteria, "condition_source_concept", None)
    if source_filter is not None:
        if isinstance(source_filter, ConceptSetSelection) or hasattr(source_filter, "codeset_id"):
            selection = source_filter
        else:
            selection = ConceptSetSelection(CodesetId=int(source_filter))
        table = apply_concept_set_selection(table, "condition_source_concept_id", selection, ctx)

    visit_source = getattr(criteria, "visit_source_concept", None)
    needs_visit_filters = bool(criteria.visit_type or criteria.visit_type_cs or visit_source is not None)
    if needs_visit_filters:
        visit = ctx.table("visit_occurrence").select(
            "visit_occurrence_id",
            "visit_concept_id",
            "visit_source_concept_id",
        )
        table = table.join(visit, table.visit_occurrence_id == visit.visit_occurrence_id)
        table = apply_visit_concept_filters(table, criteria.visit_type, criteria.visit_type_cs, ctx)
        if visit_source is not None:
            table = table.filter(table.visit_source_concept_id == int(visit_source))

    if criteria.first:
        table = apply_first_event(table, criteria.get_start_date_column(), criteria.get_primary_key_column())

    events = standardize_output(
        table,
        primary_key=criteria.get_primary_key_column(),
        start_column=criteria.get_start_date_column(),
        end_column=criteria.get_end_date_column(),
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)
