#!/usr/bin/env python

from __future__ import annotations

import argparse
from pathlib import Path

import ibis

from mitos.build_context import CohortBuildOptions, BuildContext, compile_codesets
from mitos.builders.pipeline import build_primary_events
from mitos.cohort_expression import CohortExpression


def parse_args():
    parser = argparse.ArgumentParser(description="Render Python cohort SQL for comparison with Circe.")
    parser.add_argument("--json", required=True, help="Path to the cohort expression JSON file.")
    parser.add_argument(
        "--cdm-db",
        required=True,
        help="DuckDB database path (e.g., cdm.duckdb or :memory:). Must contain OMOP CDM tables.",
    )
    parser.add_argument("--cdm-schema", help="Name of the CDM schema (optional).")
    parser.add_argument("--vocab-schema", help="Name of the vocabulary schema (optional).")
    parser.add_argument(
        "--temp-schema",
        help="Optional schema to emulate temp tables (useful for backends without true TEMP support).",
    )
    parser.add_argument("--output", help="Optional file to write the generated SQL.")
    return parser.parse_args()


def main():
    args = parse_args()
    expression = CohortExpression.model_validate_json(Path(args.json).read_text())
    conn = ibis.duckdb.connect(args.cdm_db)
    options = CohortBuildOptions(
        cdm_schema=args.cdm_schema,
        vocabulary_schema=args.vocab_schema,
        temp_emulation_schema=args.temp_schema,
    )
    codeset_resource = compile_codesets(conn, expression.concept_sets, options)
    ctx = BuildContext(conn, options, codeset_resource)

    events = build_primary_events(expression, ctx)
    if events is None:
        raise SystemExit("Cohort expression did not contain any primary criteria.")

    sql = str(events.compile())
    if args.output:
        Path(args.output).write_text(sql)
    else:
        print(sql)


if __name__ == "__main__":
    main()
