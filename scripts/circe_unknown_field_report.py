from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from mitos.testing.circe_unknown_fields import iter_unknown_circe_fields
from mitos.testing.field_usage import load_sweep_report


DEFAULT_IGNORED_KEYS = {
    # Common metadata key in exported cohort JSONs; not part of CirceR's cohortdefinition model.
    "CohortExpression.cdmVersionRange",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Report JSON properties present in cohort definitions that are not part of the "
            "Circe inventory (i.e. likely ignored by Circe)."
        )
    )
    parser.add_argument(
        "--sweep",
        type=Path,
        default=Path("report_sweep_new.json"),
        help="Path to sweep report JSON containing json_path entries.",
    )
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("tests/scenarios/fieldcases/circe_field_inventory.json"),
        help="Path to Circe field inventory JSON (usually generated from the CirceR jar).",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("reports/circe_unknown_fields.json"),
        help="Where to write the unknown-field report JSON.",
    )
    parser.add_argument(
        "--ignore-key",
        action="append",
        default=[],
        help="Fully-qualified key to ignore (repeatable), e.g. `CohortExpression.cdmVersionRange`.",
    )
    parser.add_argument(
        "--no-default-ignore",
        action="store_true",
        help="Do not ignore the built-in list of known metadata keys.",
    )
    args = parser.parse_args(argv)

    sweep_rows = load_sweep_report(args.sweep)
    circe_inventory = json.loads(args.inventory.read_text(encoding="utf-8"))

    ignored = set(args.ignore_key)
    if not args.no_default_ignore:
        ignored |= DEFAULT_IGNORED_KEYS

    occurrences: dict[str, dict[str, object]] = {}
    used_in: dict[str, set[str]] = defaultdict(set)
    examples: dict[str, list[dict[str, str]]] = defaultdict(list)

    phenotypes_with_unknown: set[str] = set()

    for row in sweep_rows:
        cohort_json = json.loads(Path(row.json_path).read_text(encoding="utf-8"))
        unknown = list(
            iter_unknown_circe_fields(
                cohort_json,
                circe_inventory=circe_inventory,
                root_class="CohortExpression",
            )
        )
        unknown = [u for u in unknown if u.key not in ignored]
        if not unknown:
            continue
        phenotypes_with_unknown.add(row.phenotype)
        for u in unknown:
            occurrences.setdefault(
                u.key,
                {
                    "class_name": u.class_name,
                    "json_property": u.json_property,
                    "used_in": 0,
                    "examples": [],
                },
            )
            used_in[u.key].add(row.phenotype)
            if len(examples[u.key]) < 10:
                examples[u.key].append(
                    {
                        "phenotype": row.phenotype,
                        "json_path": str(row.json_path),
                        "json_location": u.json_path,
                    }
                )

    for key, phenos in used_in.items():
        occurrences[key]["used_in"] = len(phenos)
        occurrences[key]["examples"] = examples[key]

    out = {
        "summary": {
            "phenotypes": len(sweep_rows),
            "phenotypes_with_unknown_fields": len(phenotypes_with_unknown),
            "unique_unknown_keys": len(occurrences),
        },
        "ignored_keys": sorted(ignored),
        "unknown_keys": dict(sorted(occurrences.items(), key=lambda kv: kv[0])),
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
