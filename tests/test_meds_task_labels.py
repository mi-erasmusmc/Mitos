from __future__ import annotations

from datetime import datetime

import ibis
import polars as pl

from mitos.meds.task_labels import (
    PlpBinaryLabelSettings,
    build_plp_binary_task_labels,
    export_meds_task_labels,
)


class _Ctx:
    def __init__(self, con: ibis.BaseBackend):
        self._con = con

    def table(self, name: str):
        return self._con.table(name)


def _dt(y, m, d):
    return datetime(y, m, d)


def test_plp_subset_labels_happy_path_and_first_exposure_only(tmp_path):
    con = ibis.duckdb.connect(database=":memory:")
    ctx = _Ctx(con)

    # Two target rows for the same subject; keep only first.
    targets = pl.DataFrame(
        {
            "person_id": [1, 1],
            "event_id": [1, 2],
            "start_date": [_dt(2020, 1, 1), _dt(2020, 2, 1)],
        }
    )
    outcomes = pl.DataFrame(
        {
            "person_id": [1],
            "start_date": [_dt(2020, 1, 2)],  # within [1,10] days of first target
        }
    )
    observation_period = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [_dt(2019, 1, 1)],
            "observation_period_end_date": [_dt(2020, 12, 31)],
        }
    )

    con.create_table("target", targets, overwrite=True)
    con.create_table("outcome", outcomes, overwrite=True)
    con.create_table("observation_period", observation_period, overwrite=True)

    settings = PlpBinaryLabelSettings(
        risk_window_start_days=1,
        risk_window_end_days=10,
        first_exposure_only=True,
        washout_period_days=0,
        remove_subjects_with_prior_outcome=True,
        prior_outcome_lookback_days=99999,
        require_time_at_risk=True,
    )

    labels = build_plp_binary_task_labels(
        ctx=ctx,
        target_rows=con.table("target"),
        outcome_rows=con.table("outcome"),
        settings=settings,
    ).to_polars()

    assert labels.shape == (1, 3)
    assert labels["subject_id"].to_list() == [1]
    assert labels["boolean_value"].to_list() == [True]

    out_dir = export_meds_task_labels(
        tmp_path,
        "demo_task",
        labels.with_columns(pl.lit("x").alias("extra")),  # prove closed-schema drop
        overwrite=True,
        shard_size=1,
        task_def={"demo": True},
    )
    assert (out_dir / "task_def.json").exists()
    assert sorted((out_dir / "labels").glob("*.parquet"))


def test_plp_subset_drops_prior_outcomes():
    con = ibis.duckdb.connect(database=":memory:")
    ctx = _Ctx(con)

    targets = pl.DataFrame(
        {
            "person_id": [1],
            "event_id": [1],
            "start_date": [_dt(2020, 1, 10)],
        }
    )
    outcomes = pl.DataFrame(
        {
            "person_id": [1, 1],
            "start_date": [_dt(2020, 1, 5), _dt(2020, 1, 12)],  # prior + in-window
        }
    )
    observation_period = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [_dt(2019, 1, 1)],
            "observation_period_end_date": [_dt(2020, 12, 31)],
        }
    )

    con.create_table("target", targets, overwrite=True)
    con.create_table("outcome", outcomes, overwrite=True)
    con.create_table("observation_period", observation_period, overwrite=True)

    settings = PlpBinaryLabelSettings(
        risk_window_start_days=1,
        risk_window_end_days=10,
        remove_subjects_with_prior_outcome=True,
        prior_outcome_lookback_days=99999,
        require_time_at_risk=True,
    )

    labels = build_plp_binary_task_labels(
        ctx=ctx,
        target_rows=con.table("target"),
        outcome_rows=con.table("outcome"),
        settings=settings,
    ).to_polars()

    assert labels.is_empty()


def test_plp_subset_drops_censored_samples():
    con = ibis.duckdb.connect(database=":memory:")
    ctx = _Ctx(con)

    targets = pl.DataFrame(
        {
            "person_id": [1],
            "event_id": [1],
            "start_date": [_dt(2020, 1, 1)],
        }
    )
    outcomes = pl.DataFrame(
        {
            "person_id": [1],
            "start_date": [_dt(2020, 1, 3)],
        }
    )
    # Observation ends too early to satisfy full TAR (risk end 10, start 1 => min TAR 9 days by default).
    observation_period = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [_dt(2019, 1, 1)],
            "observation_period_end_date": [_dt(2020, 1, 5)],
        }
    )

    con.create_table("target", targets, overwrite=True)
    con.create_table("outcome", outcomes, overwrite=True)
    con.create_table("observation_period", observation_period, overwrite=True)

    settings = PlpBinaryLabelSettings(
        risk_window_start_days=1,
        risk_window_end_days=10,
        require_time_at_risk=True,
        min_time_at_risk_days=None,  # default = 9
    )

    labels = build_plp_binary_task_labels(
        ctx=ctx,
        target_rows=con.table("target"),
        outcome_rows=con.table("outcome"),
        settings=settings,
    ).to_polars()

    assert labels.is_empty()


def test_plp_subset_include_all_outcomes_keeps_censored_positives():
    con = ibis.duckdb.connect(database=":memory:")
    ctx = _Ctx(con)

    targets = pl.DataFrame(
        {
            "person_id": [1],
            "event_id": [1],
            "start_date": [_dt(2020, 1, 1)],
        }
    )
    outcomes = pl.DataFrame(
        {
            "person_id": [1],
            "start_date": [_dt(2020, 1, 3)],  # outcome occurs early
        }
    )
    # Observation ends too early to satisfy full TAR.
    observation_period = pl.DataFrame(
        {
            "person_id": [1],
            "observation_period_start_date": [_dt(2019, 1, 1)],
            "observation_period_end_date": [_dt(2020, 1, 5)],
        }
    )

    con.create_table("target", targets, overwrite=True)
    con.create_table("outcome", outcomes, overwrite=True)
    con.create_table("observation_period", observation_period, overwrite=True)

    settings = PlpBinaryLabelSettings(
        risk_window_start_days=1,
        risk_window_end_days=10,
        require_time_at_risk=True,
        include_all_outcomes=True,
        min_time_at_risk_days=None,  # default = 9
    )

    labels = build_plp_binary_task_labels(
        ctx=ctx,
        target_rows=con.table("target"),
        outcome_rows=con.table("outcome"),
        settings=settings,
    ).to_polars()

    assert labels.shape == (1, 3)
    assert labels["boolean_value"].to_list() == [True]
