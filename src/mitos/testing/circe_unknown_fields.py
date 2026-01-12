from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from mitos.testing.circe_json_walk import _base_java_type, _inventory_index


@dataclass(frozen=True)
class UnknownCirceField:
    key: str
    class_name: str
    json_property: str
    json_path: str


def iter_unknown_circe_fields(
    cohort_json: dict[str, Any],
    *,
    circe_inventory: dict[str, list[dict[str, str]]],
    root_class: str = "CohortExpression",
) -> Iterable[UnknownCirceField]:
    """
    Yield fields found in `cohort_json` that are *not* present in the Circe inventory.

    This is a best-effort structural walk:
    - Only reports "unknown keys" for objects whose Circe class can be resolved via the inventory.
    - For Criteria wrapper objects (e.g. {"ConditionOccurrence": {...}}), reports unknown criteria types
      as `Criteria.<TypeName>`.
    """
    fields_by_class, nested_class_by_field = _inventory_index(circe_inventory)

    def walk_obj(obj: Any, class_name: str, path: str) -> Iterable[UnknownCirceField]:
        if not isinstance(obj, dict):
            return

        fields = fields_by_class.get(class_name)
        if not fields:
            return

        allowed_props = {f.json_property for f in fields}
        for k in obj.keys():
            if k not in allowed_props:
                yield UnknownCirceField(
                    key=f"{class_name}.{k}",
                    class_name=class_name,
                    json_property=k,
                    json_path=f"{path}.{k}" if path else k,
                )

        for f in fields:
            prop = f.json_property
            if prop not in obj:
                continue

            value = obj.get(prop)
            nested = nested_class_by_field.get((class_name, prop))
            base = _base_java_type(f.java_type)

            if base == "Criteria":
                # Wrapper: {"ConditionOccurrence": {...}} etc.
                if isinstance(value, dict):
                    for crit_type, crit_payload in value.items():
                        if crit_type in fields_by_class:
                            yield from walk_obj(
                                crit_payload, crit_type, f"{path}.{prop}.{crit_type}"
                            )
                        else:
                            yield UnknownCirceField(
                                key=f"Criteria.{crit_type}",
                                class_name="Criteria",
                                json_property=crit_type,
                                json_path=f"{path}.{prop}.{crit_type}",
                            )
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if not isinstance(item, dict):
                            continue
                        for crit_type, crit_payload in item.items():
                            if crit_type in fields_by_class:
                                yield from walk_obj(
                                    crit_payload, crit_type, f"{path}.{prop}[{i}].{crit_type}"
                                )
                            else:
                                yield UnknownCirceField(
                                    key=f"Criteria.{crit_type}",
                                    class_name="Criteria",
                                    json_property=crit_type,
                                    json_path=f"{path}.{prop}[{i}].{crit_type}",
                                )
                continue

            if base == "Criteria[]":
                base = "Criteria"

            if nested:
                if isinstance(value, dict):
                    yield from walk_obj(value, nested, f"{path}.{prop}")
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        yield from walk_obj(item, nested, f"{path}.{prop}[{i}]")

    yield from walk_obj(cohort_json, root_class, root_class)

