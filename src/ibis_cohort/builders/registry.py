from __future__ import annotations

from collections.abc import Callable
from typing import Dict, Type

import ibis.expr.types as ir

from ibis_cohort.build_context import BuildContext
from ibis_cohort.criteria import Criteria

_REGISTRY: Dict[str, Callable[[Criteria, BuildContext], ir.Table]] = {}


def register(criteria_name: str):
    def decorator(func: Callable[[Criteria, BuildContext], ir.Table]):
        _REGISTRY[criteria_name] = func
        return func

    return decorator


def get_builder(criteria: Criteria):
    name = criteria.__class__.__name__
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"No builder registered for criteria {name}") from exc


def build_events(criteria: Criteria, ctx: BuildContext) -> ir.Table:
    builder = get_builder(criteria)
    return builder(criteria, ctx)
