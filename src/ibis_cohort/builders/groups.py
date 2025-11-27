from __future__ import annotations

import ibis
import ibis.expr.types as ir

from ibis_cohort.build_context import BuildContext
from ibis_cohort.builders.registry import build_events
from ibis_cohort.builders.common import (
    apply_age_filter,
    apply_gender_filter,
    apply_visit_concept_filters,
    apply_race_filter,
    apply_ethnicity_filter,
    apply_date_range,
    apply_observation_window,
)
from ibis_cohort.criteria import (
    CriteriaGroup,
    CorrelatedCriteria,
    OccurrenceType,
    DemoGraphicCriteria,
    CriteriaColumn,
)
from ibis_cohort.tables import parse_single_criteria, VisitDetail
from ibis_cohort.cohort_expression import ObservationFilter


def apply_criteria_group(events: ir.Table, group: CriteriaGroup | None, ctx: BuildContext) -> ir.Table:
    mask = _group_mask(events, group, ctx)
    if mask is None:
        return events
    return events.filter(mask)


def _correlated_mask(events: ir.Table, correlated: CorrelatedCriteria, ctx: BuildContext) -> ir.Value:
    criteria_model = correlated.criteria
    if criteria_model and not isinstance(criteria_model, ir.Expr):
        criteria_model = parse_single_criteria(criteria_model)
    if criteria_model is None:
        return ibis.literal(True)

    base_events = build_events(criteria_model, ctx)
    if not correlated.ignore_observation_period:
        zero_window = ObservationFilter(prior_days=0, post_days=0)
        base_events = apply_observation_window(base_events, zero_window, ctx)

    select_fields = [
        base_events.person_id,
        base_events.event_id.name("_corr_event_id"),
        base_events.start_date.name("_corr_start_date"),
        base_events.end_date.name("_corr_end_date"),
    ]
    if "visit_occurrence_id" in base_events.columns:
        select_fields.append(base_events.visit_occurrence_id.name("_corr_visit_occurrence_id"))

    criteria_events = base_events.select(*select_fields)
    join_condition = events.person_id == criteria_events.person_id
    if not correlated.ignore_observation_period:
        if "observation_period_start_date" in events.columns:
            join_condition &= criteria_events._corr_start_date >= events.observation_period_start_date
        if "observation_period_end_date" in events.columns:
            join_condition &= criteria_events._corr_start_date <= events.observation_period_end_date
            join_condition &= criteria_events._corr_end_date <= events.observation_period_end_date
    window_condition = _build_window_condition(events, criteria_events, correlated)
    if window_condition is not None:
        join_condition &= window_condition

    occurrence = correlated.occurrence
    occ_type = getattr(occurrence, "type", None)
    if isinstance(occ_type, int):
        occ_type = OccurrenceType(occurrence.type)

    require_same_visit = bool(correlated.restrict_visit)
    if correlated.restrict_visit is None and isinstance(criteria_model, VisitDetail):
        require_same_visit = True

    if require_same_visit:
        if "visit_occurrence_id" in events.columns and "_corr_visit_occurrence_id" in criteria_events.columns:
            join_condition &= (
                events.visit_occurrence_id.notnull()
                & criteria_events._corr_visit_occurrence_id.notnull()
                & (events.visit_occurrence_id == criteria_events._corr_visit_occurrence_id)
            )

    joined = events.join(criteria_events, join_condition, how="left")

    count_column_name = _resolve_count_column_name(correlated.occurrence)
    count_expr = criteria_events._corr_event_id
    if count_column_name and count_column_name in joined.columns:
        count_expr = joined[count_column_name]
    match_expr = criteria_events._corr_event_id.notnull()
    joined = joined.mutate(
        _corr_match_value=ibis.ifelse(match_expr, count_expr, ibis.null()),
    )

    if correlated.occurrence and correlated.occurrence.is_distinct:
        aggregator = joined._corr_match_value.nunique()
    else:
        aggregator = joined._corr_match_value.count()

    aggregated = joined.group_by(joined.event_id).aggregate(match_count=aggregator)
    predicate = _occurrence_predicate(aggregated.match_count, correlated.occurrence)
    matching_ids = aggregated.filter(predicate).select("event_id")
    return events.event_id.isin(matching_ids.event_id)


def _group_mask(events: ir.Table, group: CriteriaGroup | None, ctx: BuildContext) -> ir.Value | None:
    if not group or group.is_empty():
        return None

    masks: list[ir.Value] = []
    for correlated in group.criteria_list:

        masks.append(_correlated_mask(events, correlated, ctx))

    for demographic in group.demographic_criteria_list:
        demo_mask = _demographic_mask(events, demographic, ctx)
        if demo_mask is not None:
            masks.append(demo_mask)

    for subgroup in group.groups:
        sub_mask = _group_mask(events, subgroup, ctx)
        if sub_mask is not None:
            masks.append(sub_mask)

    if not masks:
        return None

    group_type = (group.type or "ALL").upper()
    if group_type == "ANY":
        return _combine_any(masks)
    if group_type.startswith("AT_"):
        count = group.count
        if group_type.endswith("LEAST"):
            threshold = count if count is not None else 1
            return _combine_threshold(masks, threshold, at_least=True)
        threshold = count if count is not None else 0
        return _combine_threshold(masks, threshold, at_least=False)
    return _combine_all(masks)


def _combine_all(masks: list[ir.Value]) -> ir.Value:
    combined = masks[0]
    for mask in masks[1:]:
        combined = combined & mask
    return combined


def _combine_any(masks: list[ir.Value]) -> ir.Value:
    combined = masks[0]
    for mask in masks[1:]:
        combined = combined | mask
    return combined


def _combine_threshold(masks: list[ir.Value], threshold: int, *, at_least: bool) -> ir.Value:
    total = masks[0].cast("int64")
    for mask in masks[1:]:
        total = total + mask.cast("int64")
    return total >= threshold if at_least else total <= threshold


def _demographic_mask(events: ir.Table, demographic: DemoGraphicCriteria, ctx: BuildContext) -> ir.Value | None:
    if demographic is None:
        return None

    filtered = events
    applied = False
    if demographic.age:
        filtered = apply_age_filter(filtered, demographic.age, ctx, "start_date")
        applied = True
    if demographic.gender or demographic.gender_cs:
        filtered = apply_gender_filter(filtered, demographic.gender, demographic.gender_cs, ctx)
        applied = True
    if demographic.race or demographic.race_cs:
        filtered = apply_race_filter(filtered, demographic.race, demographic.race_cs, ctx)
        applied = True
    if demographic.ethnicity or demographic.ethnicity_cs:
        filtered = apply_ethnicity_filter(filtered, demographic.ethnicity, demographic.ethnicity_cs, ctx)
        applied = True
    if demographic.occurrence_start_date:
        filtered = apply_date_range(filtered, "start_date", demographic.occurrence_start_date)
        applied = True
    if demographic.occurrence_end_date:
        filtered = apply_date_range(filtered, "end_date", demographic.occurrence_end_date)
        applied = True

    if not applied:
        return None

    filtered_ids = filtered.select(filtered.event_id).distinct()
    return events.event_id.isin(filtered_ids.event_id)


def _occurrence_predicate(count_expr: ir.Value, occurrence) -> ir.Value:
    if occurrence is None:
        return count_expr > 0

    occ_type = occurrence.type
    if isinstance(occ_type, int):
        occ_type = OccurrenceType(occurrence.type)

    if occ_type == OccurrenceType.EXACTLY:
        return count_expr == occurrence.count
    if occ_type == OccurrenceType.AT_LEAST:
        return count_expr >= occurrence.count
    if occ_type == OccurrenceType.AT_MOST:
        return count_expr <= occurrence.count
    return count_expr > 0


def _build_window_condition(index_events: ir.Table, correlated_events: ir.Table, correlated: CorrelatedCriteria) -> ir.Value:
    cond = ibis.literal(True)

    if correlated.start_window:
        correlated_start = _correlated_window_value(
            correlated_events,
            correlated.start_window.use_event_end,
            default="start",
        )
        lower = _apply_endpoint_anchor(index_events, correlated.start_window.start, correlated.start_window.use_index_end)
        upper = _apply_endpoint_anchor(index_events, correlated.start_window.end, correlated.start_window.use_index_end)
        if lower is not None:
            cond &= correlated_start >= lower
        if upper is not None:
            cond &= correlated_start <= upper

    if correlated.end_window:
        lower = _apply_endpoint_anchor(
            index_events,
            correlated.end_window.start,
            correlated.end_window.use_index_end,
            default_to_index_end=False,
        )
        upper = _apply_endpoint_anchor(
            index_events,
            correlated.end_window.end,
            correlated.end_window.use_index_end,
            default_to_index_end=False,
        )
        correlated_end = _correlated_window_value(
            correlated_events,
            correlated.end_window.use_event_end,
            default="end",
        )
        if lower is not None:
            cond &= correlated_end >= lower
        if upper is not None:
            cond &= correlated_end <= upper

    return cond


def _apply_endpoint_anchor(
    events: ir.Table,
    endpoint,
    use_index_end: bool | None,
    *,
    default_to_index_end: bool = False,
):
    anchor = events.end_date if (use_index_end or (use_index_end is None and default_to_index_end)) else events.start_date
    if not endpoint or endpoint.days is None:
        return None
    days = ibis.interval(days=int(endpoint.days))
    coeff = endpoint.coeff if endpoint.coeff is not None else 1
    return anchor + days * coeff


def _correlated_window_value(
    correlated_events: ir.Table,
    use_event_end: bool | None,
    *,
    default: str,
) -> ir.Value:
    if use_event_end:
        return correlated_events._corr_end_date
    if default == "end":
        return correlated_events._corr_end_date
    return correlated_events._corr_start_date


_COUNT_COLUMN_MAPPING: dict[CriteriaColumn, str] = {
    CriteriaColumn.START_DATE: "_corr_start_date",
    CriteriaColumn.END_DATE: "_corr_end_date",
    CriteriaColumn.VISIT_ID: "_corr_visit_occurrence_id",
}


def _resolve_count_column_name(occurrence) -> str | None:
    if occurrence is None or occurrence.count_column is None:
        return None
    column = occurrence.count_column
    enum_value: CriteriaColumn | None = None
    if isinstance(column, CriteriaColumn):
        enum_value = column
    else:
        value = str(column)
        if value.upper() in CriteriaColumn.__members__:
            enum_value = CriteriaColumn[value.upper()]
        else:
            lower = value.lower()
            for member in CriteriaColumn:
                if member.value == lower:
                    enum_value = member
                    break
    if enum_value is None:
        return None
    return _COUNT_COLUMN_MAPPING.get(enum_value)
