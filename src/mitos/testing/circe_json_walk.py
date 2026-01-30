from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class CirceInventoryField:
    json_property: str
    java_type: str


_LIST_RE = re.compile(r"^List<(?P<inner>[^>]+)>$")


def _base_java_type(java_type: str) -> str:
    t = java_type.strip()
    if t.endswith("[]"):
        return t[:-2]
    m = _LIST_RE.match(t)
    if m:
        return m.group("inner").strip()
    return t


def _inventory_index(
    circe_inventory: dict[str, list[dict[str, str]]],
) -> tuple[dict[str, list[CirceInventoryField]], dict[tuple[str, str], str]]:
    """
    Return:
      - fields_by_class: class -> [fields]
      - nested_class_by_field: (class, json_property) -> nested_class_name (if resolvable)
    """
    fields_by_class: dict[str, list[CirceInventoryField]] = {}
    nested_class_by_field: dict[tuple[str, str], str] = {}

    classes = set(circe_inventory.keys())
    for class_name, fields in circe_inventory.items():
        out_fields: list[CirceInventoryField] = []
        for entry in fields:
            prop = entry["json_property"]
            java_type = entry["java_type"]
            out_fields.append(
                CirceInventoryField(json_property=prop, java_type=java_type)
            )

            base = _base_java_type(java_type)
            if base in classes:
                nested_class_by_field[(class_name, prop)] = base
        fields_by_class[class_name] = out_fields

    return fields_by_class, nested_class_by_field


def iter_circe_inventory_fields_present(
    cohort_json: dict[str, Any],
    *,
    circe_inventory: dict[str, list[dict[str, str]]],
    root_class: str = "CohortExpression",
) -> Iterable[str]:
    """
    Yield `ClassName.JsonProperty` strings for every @JsonProperty encountered in `cohort_json`.

    Unlike naive "dict key == class name" scanning, this uses Java type information
    from `circe_inventory` to follow nested objects:
      - CohortExpression.PrimaryCriteria -> PrimaryCriteria
      - ConditionOccurrence.CorrelatedCriteria -> CorelatedCriteria
      - CorelatedCriteria.StartWindow -> Window
      - PrimaryCriteria.CriteriaList -> Criteria[] (special wrapper objects)
      - CriteriaGroup.CriteriaList -> CorelatedCriteria[]
    """
    fields_by_class, nested_class_by_field = _inventory_index(circe_inventory)

    def walk_obj(obj: Any, class_name: str) -> Iterable[str]:
        if not isinstance(obj, dict):
            return

        fields = fields_by_class.get(class_name, [])
        for f in fields:
            prop = f.json_property
            if prop not in obj:
                continue
            yield f"{class_name}.{prop}"

            value = obj.get(prop)
            nested = nested_class_by_field.get((class_name, prop))
            base = _base_java_type(f.java_type)

            if base == "Criteria":
                # Wrapper: {"ConditionOccurrence": {...}} etc.
                if isinstance(value, dict):
                    for k, v in value.items():
                        if k in fields_by_class:
                            yield from walk_obj(v, k)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k in fields_by_class:
                                    yield from walk_obj(v, k)
                continue

            if base == "Criteria[]":
                # Not expected, but keep for safety.
                base = "Criteria"

            if nested:
                if isinstance(value, dict):
                    yield from walk_obj(value, nested)
                elif isinstance(value, list):
                    for item in value:
                        yield from walk_obj(item, nested)

    yield from walk_obj(cohort_json, root_class)
