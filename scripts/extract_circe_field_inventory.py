from __future__ import annotations

import argparse
from pathlib import Path

from mitos.testing.circe_inventory import extract_circe_field_inventory, find_circe_jar, write_circe_field_inventory


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract Circe cohort-definition JSON property inventory.")
    parser.add_argument(
        "--circe-src",
        type=Path,
        default=None,
        help="Path to Circe's `org/ohdsi/circe/cohortdefinition` Java directory (or a Circe jar).",
    )
    parser.add_argument(
        "--circe-jar",
        type=Path,
        default=None,
        help="Path to a Circe jar. When omitted, tries to locate CirceR's jar via Rscript.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("tests/scenarios/fieldcases/circe_field_inventory.json"),
        help="Where to write the extracted inventory JSON.",
    )
    args = parser.parse_args(argv)

    if args.circe_jar is not None:
        source = args.circe_jar
    elif args.circe_src is not None:
        source = args.circe_src
    else:
        source = find_circe_jar()

    inventory = extract_circe_field_inventory(source)
    write_circe_field_inventory(args.out, inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
