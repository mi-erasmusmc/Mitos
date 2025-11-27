import argparse
import json

import ibis

from . import builders as _builders  # noqa: F401
from .cohort_expression import CohortExpression
from .build_context import BuildContext, CohortBuildOptions, compile_codesets
from .builders.pipeline import build_primary_events, build_primary_events_polars


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compile primary criteria query for a cohort JSON.")
    parser.add_argument(
        "--json",
        default="cohorts/6243-dementia-outcome-v1.json",
        help="Path to the CohortDefinition JSON exported from ATLAS.",
    )
    parser.add_argument(
        "--connection",
        default="duckdb:///home/egill/database/database.duckdb",
        help="Ibis connection string (DuckDB by default).",
    )
    parser.add_argument(
        "--mode",
        choices=("polars", "sql"),
        default="polars",
        help="Render results as a Polars DataFrame or print the generated SQL.",
    )
    args = parser.parse_args(argv)

    with open(args.json, "r") as fh:
        json_str = fh.read()
    cohort_expression = CohortExpression.model_validate_json(json_str)

    conn = ibis.connect(args.connection)
    options = CohortBuildOptions()
    codeset_resource = compile_codesets(conn, cohort_expression.concept_sets, options)
    context = BuildContext(conn, options, codeset_resource)
    try:
        if args.mode == "sql":
            events = build_primary_events(cohort_expression, context)
            if events is None:
                print("No primary events were generated.")  # noqa: T201
                return 0
            print(events.compile())  # noqa: T201
        else:
            df = build_primary_events_polars(cohort_expression, context)
            if df.is_empty():
                print("No primary events were generated.")  # noqa: T201
                return 0
            print(df)  # noqa: T201
        return 0
    finally:
        context.close()


if __name__ == "__main__":
    raise SystemExit(main())
