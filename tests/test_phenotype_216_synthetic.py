from __future__ import annotations

from datetime import date

import ibis

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events

from tests.scenarios.phenotype_216 import build_fake_omop_for_phenotype_216


def test_phenotype_216_synthetic_dataset_expected_people_and_censoring():
    con = ibis.duckdb.connect(database=":memory:")

    expression, expectations = build_fake_omop_for_phenotype_216(con, schema="main")

    options = CohortBuildOptions(
        cdm_schema="main",
        vocabulary_schema="main",
        backend="duckdb",
        materialize_stages=False,
        materialize_codesets=True,
    )
    codesets = compile_codesets(con, expression.concept_sets, options)
    ctx = BuildContext(con, options, codesets)
    try:
        events = build_primary_events(expression, ctx)
        assert events is not None

        df = events.execute()
        got_people = set(int(v) for v in df["person_id"].tolist())

        assert expectations.include_person_ids.issubset(got_people)
        assert got_people.isdisjoint(expectations.exclude_person_ids)
        assert expectations.neutrophil_cells_unit_fail_person_id not in got_people

        # Washout person has 2 index events; only 1 should remain after inclusion rules.
        washout_count = int((df["person_id"] == expectations.washout_person_id).sum())
        assert washout_count == 1

        # Washout boundary is inclusive at -365d: second event should be removed.
        washout_boundary_count = int(
            (df["person_id"] == expectations.washout_boundary_person_id).sum()
        )
        assert washout_boundary_count == 1

        # Outside washout window: both events should remain.
        washout_outside_count = int(
            (df["person_id"] == expectations.washout_outside_person_id).sum()
        )
        assert washout_outside_count == 2

        # End-strategy should cap at observation period end date.
        cap_rows = df[df["person_id"] == expectations.strategy_cap_person_id]
        assert len(cap_rows) == 1
        cap_end = cap_rows.iloc[0]["end_date"]
        if hasattr(cap_end, "date"):
            cap_end = cap_end.date()
        assert cap_end == expectations.strategy_cap_expected_end_date

        # Censoring should cut end_date to the censor date.
        censor_rows = df[df["person_id"] == expectations.censor_person_id]
        assert len(censor_rows) == 1
        end_dt = censor_rows.iloc[0]["end_date"]
        if hasattr(end_dt, "date"):
            end_dt = end_dt.date()
        assert end_dt == expectations.censor_expected_end_date
        assert isinstance(end_dt, date)
    finally:
        ctx.close()
