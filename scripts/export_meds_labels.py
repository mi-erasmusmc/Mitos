#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import ibis

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events
from mitos.cohort_expression import CohortExpression
from mitos.meds.task_labels import (
    PlpBinaryLabelSettings,
    build_plp_binary_task_labels,
    export_meds_task_labels,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Export PLP-style binary task labels in MEDS format."
    )
    p.add_argument(
        "--connection",
        required=True,
        help="Ibis connection string (e.g. duckdb:///path/db.duckdb).",
    )
    p.add_argument(
        "--cdm-schema",
        default=None,
        help="OMOP CDM schema/database (ibis `database=`).",
    )
    p.add_argument(
        "--vocab-schema",
        default=None,
        help="OMOP vocab schema/database (defaults to cdm-schema).",
    )

    p.add_argument(
        "--target-json",
        required=True,
        help="ATLAS cohort JSON for the target/index cohort.",
    )
    p.add_argument(
        "--outcome-json",
        required=True,
        help="ATLAS cohort JSON for the outcome cohort.",
    )

    p.add_argument("--task-root", required=True, help="Root directory for MEDS tasks.")
    p.add_argument(
        "--task-name", required=True, help="Task name directory under task-root."
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing task directory if present.",
    )
    p.add_argument(
        "--shard-size", type=int, default=250_000, help="Rows per Parquet shard."
    )

    p.add_argument("--risk-window-start", type=int, default=1)
    p.add_argument("--risk-window-end", type=int, default=365)
    p.add_argument("--min-time-at-risk", type=int, default=None)
    p.add_argument("--washout-period", type=int, default=0)
    p.add_argument("--first-exposure-only", action="store_true")
    p.add_argument(
        "--keep-prior-outcomes",
        action="store_true",
        help="Do not remove subjects with prior outcomes.",
    )
    p.add_argument(
        "--include-all-outcomes",
        action="store_true",
        help="Keep outcome cases even if they do not have sufficient observable time-at-risk (PLP includeAllOutcomes=TRUE).",
    )
    p.add_argument("--prior-outcome-lookback", type=int, default=99999)

    args = p.parse_args(argv)

    conn = ibis.connect(args.connection)

    options = CohortBuildOptions(
        cdm_schema=args.cdm_schema,
        vocabulary_schema=args.vocab_schema,
    )

    target_expr = CohortExpression.model_validate_json(
        Path(args.target_json).read_text(encoding="utf-8")
    )
    outcome_expr = CohortExpression.model_validate_json(
        Path(args.outcome_json).read_text(encoding="utf-8")
    )

    target_codesets = compile_codesets(conn, target_expr.concept_sets, options)
    outcome_codesets = compile_codesets(conn, outcome_expr.concept_sets, options)

    ctx_target = BuildContext(conn, options, target_codesets)
    ctx_outcome = BuildContext(conn, options, outcome_codesets)
    try:
        target_rows = build_primary_events(target_expr, ctx_target)
        if target_rows is None:
            raise SystemExit("No target rows were generated.")
        outcome_rows = build_primary_events(outcome_expr, ctx_outcome)
        if outcome_rows is None:
            raise SystemExit("No outcome rows were generated.")

        settings = PlpBinaryLabelSettings(
            risk_window_start_days=args.risk_window_start,
            risk_window_end_days=args.risk_window_end,
            first_exposure_only=bool(args.first_exposure_only),
            washout_period_days=args.washout_period,
            remove_subjects_with_prior_outcome=not bool(args.keep_prior_outcomes),
            prior_outcome_lookback_days=args.prior_outcome_lookback,
            require_time_at_risk=True,
            include_all_outcomes=bool(args.include_all_outcomes),
            min_time_at_risk_days=args.min_time_at_risk,
        )

        labels_expr = build_plp_binary_task_labels(
            ctx=ctx_target,
            target_rows=target_rows,
            outcome_rows=outcome_rows,
            settings=settings,
        )

        labels_df = labels_expr.to_polars()

        task_def = {
            "format": "meds.task_labels.v1",
            "label_type": "boolean_value",
            "plp_subset": True,
            "settings": {
                "risk_window_start_days": settings.risk_window_start_days,
                "risk_window_end_days": settings.risk_window_end_days,
                "min_time_at_risk_days": settings.effective_min_time_at_risk_days(),
                "washout_period_days": settings.washout_period_days,
                "first_exposure_only": settings.first_exposure_only,
                "include_all_outcomes": settings.include_all_outcomes,
                "remove_subjects_with_prior_outcome": settings.remove_subjects_with_prior_outcome,
                "prior_outcome_lookback_days": settings.prior_outcome_lookback_days,
            },
            "inputs": {
                "target_json": str(Path(args.target_json)),
                "outcome_json": str(Path(args.outcome_json)),
            },
        }

        out_dir = export_meds_task_labels(
            args.task_root,
            args.task_name,
            labels_df,
            shard_size=args.shard_size,
            overwrite=bool(args.overwrite),
            task_def=task_def,
        )
        print(
            json.dumps(
                {"task_dir": str(out_dir), "rows": int(labels_df.height)}, indent=2
            )
        )  # noqa: T201
        return 0
    finally:
        ctx_outcome.close()
        ctx_target.close()


if __name__ == "__main__":
    raise SystemExit(main())
