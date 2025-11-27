#!/usr/bin/env python

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import ibis

from ibis_cohort.cohort_expression import CohortExpression
from ibis_cohort.build_context import CohortBuildOptions, BuildContext, compile_codesets
from ibis_cohort.builders.pipeline import build_primary_events


def profile_expression(conn, expression: CohortExpression, options: CohortBuildOptions):
    compile_start = time.perf_counter()
    resource = compile_codesets(conn, expression.concept_sets, options)
    compile_ms = (time.perf_counter() - compile_start) * 1000

    ctx = BuildContext(conn, options, resource)
    try:
        build_start = time.perf_counter()
        events = build_primary_events(expression, ctx)
        build_ms = (time.perf_counter() - build_start) * 1000
        if events is None:
            return {
                "compile_ms": compile_ms,
                "build_ms": build_ms,
                "sql_lines": 0,
                "plan_lines": 0,
                "row_count": 0,
            }
        sql = events.compile()
        sql_chars = len(sql)
        plan_rows = conn.raw_sql(f"EXPLAIN {sql}").fetchall()
        plan_text = "\n".join(
            str(row[1]) if isinstance(row, tuple) and len(row) > 1 else str(row[0])
            for row in plan_rows
        )
        plan_lines = len(plan_text.splitlines()) or 1
        row_count = None
        count_ms = None
        if options.generate_stats:
            count_start = time.perf_counter()
            row_count = conn.raw_sql(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
            count_ms = (time.perf_counter() - count_start) * 1000
        return {
            "compile_ms": compile_ms,
            "build_ms": build_ms,
            "sql_chars": sql_chars,
            "plan_lines": plan_lines,
            "row_count": row_count,
            "count_ms": count_ms,
        }
    finally:
        ctx.close()


def main():
    parser = argparse.ArgumentParser(description="Profile phenotype SQL size and plan complexity.")
    parser.add_argument(
        "--db",
        default="/home/egill/database/database-1M_filtered.duckdb",
        help="DuckDB database path",
    )
    parser.add_argument(
        "--fixtures",
        default="fixtures/phenotypes",
        help="Directory containing phenotype JSON files",
    )
    parser.add_argument(
        "--phenotypes",
        nargs="*",
        default=["phenotype-2.json", "phenotype-30.json", "phenotype-78.json", "phenotype-344.json", "phenotype-500.json"],
        help="Phenotype JSON filenames relative to fixtures directory",
    )
    parser.add_argument("--output", help="Optional path to write JSON results")
    parser.add_argument(
        "--row-count",
        action="store_true",
        help="Execute cohort SQL to compute row counts (can be slow)",
    )
    args = parser.parse_args()

    fixtures_dir = Path(args.fixtures)
    results = []
    for name in args.phenotypes:
        path = fixtures_dir / name
        if not path.exists():
            print(f"Skipping missing phenotype {path}")
            continue
        expression = CohortExpression.model_validate_json(path.read_text())
        conn = ibis.duckdb.connect(database=args.db)
        try:
            metrics = profile_expression(conn, expression, CohortBuildOptions(generate_stats=args.row_count))
        finally:
            if hasattr(conn, "close"):
                conn.close()
        results.append({"phenotype": name, **metrics})
        print(f"{name}: sql_chars={metrics['sql_chars']} plan_lines={metrics['plan_lines']} rows={metrics['row_count']}")

    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
