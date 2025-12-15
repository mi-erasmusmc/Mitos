#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
for p in (REPO_ROOT, REPO_ROOT / "src", REPO_ROOT / "tests"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import ibis  # noqa: E402

from tests.scenarios.phenotype_216 import build_fake_omop_for_phenotype_216  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a synthetic OMOP CDM DuckDB for a phenotype scenario.")
    parser.add_argument("--out-db", required=True, help="Path to write the DuckDB database file.")
    parser.add_argument("--schema", default="main", help="DuckDB schema/database name to write tables into.")
    parser.add_argument("--phenotype", default="216", help="Phenotype id (currently only '216' supported).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_db = Path(args.out_db)
    out_db.parent.mkdir(parents=True, exist_ok=True)

    con = ibis.duckdb.connect(database=str(out_db))
    try:
        if args.phenotype != "216":
            raise SystemExit("Only phenotype '216' is supported by this generator right now.")
        build_fake_omop_for_phenotype_216(con, schema=args.schema)
    finally:
        # Ibis backend objects don't always expose a uniform close() API.
        if hasattr(con, "close"):
            con.close()
        elif hasattr(con, "disconnect"):
            con.disconnect()
        else:
            underlying = getattr(con, "con", None)
            if underlying is not None and hasattr(underlying, "close"):
                underlying.close()

    print(f"Wrote synthetic OMOP tables to: {out_db}")
    print("\nSuggested `profiles.yaml` snippet:")
    print(f"""
fake_{args.phenotype}:
  backend: duckdb
  database: "{out_db}"
  read_only: false
""".strip())
    print()
    print("Run:")
    print(f"  .venv/bin/python scripts/compare_cohort_counts.py --profile fake_{args.phenotype} --json cohorts/phenotype-{args.phenotype}.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
