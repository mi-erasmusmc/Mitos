from __future__ import annotations

from mitos.build_context import BuildContext
from mitos.tables import Measurement
from mitos.criteria import ConceptSetSelection
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
import ibis


@register("Measurement")
def build_measurement(criteria: Measurement, ctx: BuildContext):
    table = ctx.table("measurement")
    concept_column = criteria.get_concept_id_column()
    table = apply_codeset_filter(table, concept_column, criteria.codeset_id, ctx)
    if criteria.first:
        table = apply_first_event(table, criteria.get_start_date_column(), criteria.get_primary_key_column())

    table = apply_date_range(table, criteria.get_start_date_column(), criteria.occurrence_start_date)
    table = apply_date_range(table, criteria.get_end_date_column(), criteria.occurrence_end_date)

    if criteria.measurement_type:
        table = apply_concept_filters(table, "measurement_type_concept_id", criteria.measurement_type)
    table = apply_concept_set_selection(table, "measurement_type_concept_id", criteria.measurement_type_cs, ctx)
    if criteria.measurement_type_exclude:
        table = apply_concept_filters(table, "measurement_type_concept_id", criteria.measurement_type, exclude=True)

    value_column = "value_as_number"
    if criteria.unit:
        table = apply_concept_filters(table, "unit_concept_id", criteria.unit)
        table, value_column = _maybe_normalize_units(table, criteria.unit)
    table = apply_concept_set_selection(table, "unit_concept_id", criteria.unit_cs, ctx)

    if criteria.value_as_concept:
        table = apply_concept_filters(table, "value_as_concept_id", criteria.value_as_concept)
    table = apply_concept_set_selection(table, "value_as_concept_id", criteria.value_as_concept_cs, ctx)

    table = apply_numeric_range(table, value_column, criteria.value_as_number)
    table = apply_numeric_range(table, "range_low", criteria.range_low)
    table = apply_numeric_range(table, "range_high", criteria.range_high)

    if criteria.age:
        table = apply_age_filter(table, criteria.age, ctx, criteria.get_start_date_column())
    table = apply_gender_filter(table, criteria.gender, criteria.gender_cs, ctx)
    table = apply_visit_concept_filters(table, criteria.visit_type, criteria.visit_type_cs, ctx)
    source_concept = getattr(criteria, "measurement_source_concept", None)
    if source_concept is not None:
        if hasattr(source_concept, "codeset_id"):
            table = apply_concept_set_selection(table, "measurement_source_concept_id", source_concept, ctx)
        else:
            table = table.filter(table.measurement_source_concept_id == int(source_concept))

    events = standardize_output(
        table,
        primary_key=criteria.get_primary_key_column(),
        start_column=criteria.get_start_date_column(),
        end_column=criteria.get_end_date_column(),
    )
    return apply_criteria_group(events, criteria.correlated_criteria, ctx)


def _maybe_normalize_units(table, units):
    unit_ids = [concept.concept_id for concept in units if concept.concept_id is not None]
    if not unit_ids:
        return table, "value_as_number"
    if not all(unit_id in _UNIT_NORMALIZATION for unit_id in unit_ids):
        return table, "value_as_number"
    groups = { _UNIT_NORMALIZATION[unit_id][0] for unit_id in unit_ids}
    if len(groups) != 1:
        return table, "value_as_number"
    multiplier = _unit_multiplier_expr(table.unit_concept_id, unit_ids)
    normalized = (table.value_as_number * multiplier).name("_normalized_value")
    table = table.mutate(_normalized_value=normalized)
    return table, "_normalized_value"


def _unit_multiplier_expr(unit_column, unit_ids):
    multiplier_expr = ibis.literal(1.0)
    for unit_id in unit_ids:
        multiplier = _UNIT_NORMALIZATION[unit_id][1]
        multiplier_expr = ibis.ifelse(
            unit_column == ibis.literal(unit_id),
            ibis.literal(multiplier),
            multiplier_expr,
        )
    return multiplier_expr


_UNIT_NORMALIZATION = {
    # Mass
    9529: ("mass_kg", 1.0),  # kilogram
    3195625: ("mass_kg", 0.45359237),  # pound
    # Cell counts per liter (expressed in 10^9/L)
    9444: ("count_10e9_per_l", 1.0),  # billion per liter
    44777588: ("count_10e9_per_l", 1.0),
    8848: ("count_10e9_per_l", 1.0),  # thousand per microliter
    8816: ("count_10e9_per_l", 1.0),  # million per milliliter
    8961: ("count_10e9_per_l", 1.0),  # thousand per cubic millimeter
    8784: ("count_10e9_per_l", 0.001),  # cells per microliter
    8647: ("count_10e9_per_l", 0.001),  # per microliter
}
