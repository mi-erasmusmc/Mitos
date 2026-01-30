from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

import ibis
import ibis.expr.types as ir
import polars as pl


@dataclass(frozen=True)
class PlpBinaryLabelSettings:
    """
    Subset of PatientLevelPrediction::createStudyPopulation settings.

    This subset is intentionally restrictive:
    - binary outcome only
    - startAnchor/endAnchor fixed to "cohort start"
    - includeAllOutcomes fixed to FALSE (drop censored)
    """

    risk_window_start_days: int = 1
    risk_window_end_days: int = 365

    first_exposure_only: bool = False
    washout_period_days: int = 0

    remove_subjects_with_prior_outcome: bool = True
    prior_outcome_lookback_days: int = 99999

    require_time_at_risk: bool = True
    include_all_outcomes: bool = False
    min_time_at_risk_days: int | None = None

    def effective_min_time_at_risk_days(self) -> int:
        if self.min_time_at_risk_days is not None:
            return int(self.min_time_at_risk_days)
        return int(self.risk_window_end_days - self.risk_window_start_days)


def build_plp_binary_task_labels(
    *,
    ctx: Any,
    target_rows: ir.Table,
    outcome_rows: ir.Table,
    settings: PlpBinaryLabelSettings,
    subject_id_col: str = "person_id",
    index_time_col: str = "start_date",
    outcome_time_col: str = "start_date",
) -> ir.Table:
    """
    Build a per-(subject_id, prediction_time) binary label table using PLP-like semantics.

    Expected inputs:
    - target_rows: one row per "exposure"/index event, with subject id + index time
    - outcome_rows: one row per outcome event, with subject id + outcome time
    - ctx: BuildContext or any object with `.table("observation_period")`

    Returns an Ibis table with MEDS-compatible columns:
      subject_id (int64), prediction_time (timestamp), boolean_value (bool)
    """
    if settings.risk_window_end_days < settings.risk_window_start_days:
        raise ValueError("risk_window_end_days must be >= risk_window_start_days")
    if settings.washout_period_days < 0:
        raise ValueError("washout_period_days must be >= 0")
    if settings.prior_outcome_lookback_days < 0:
        raise ValueError("prior_outcome_lookback_days must be >= 0")
    if settings.require_time_at_risk and settings.effective_min_time_at_risk_days() < 0:
        raise ValueError("min_time_at_risk_days must be >= 0")

    def _ensure_timestamp(expr: ir.Value) -> ir.Value:
        dtype = expr.type()
        if dtype.is_timestamp():
            return expr
        if dtype.is_date():
            return expr.cast("timestamp")
        if dtype.is_string():
            return ibis.to_timestamp(expr)
        raise ValueError(f"Cannot convert expression of type {dtype} to timestamp")

    target = target_rows.view()
    subject = target[subject_id_col].cast("int64").name("subject_id")
    prediction_time = _ensure_timestamp(target[index_time_col]).name("prediction_time")

    # Use a stable per-person row key if present; otherwise synthesize one.
    if "event_id" in target.columns:
        row_id = target["event_id"].cast("int64").name("row_id")
    else:
        w = ibis.window(group_by=subject, order_by=[prediction_time])
        row_id = (ibis.row_number().over(w) + 1).cast("int64").name("row_id")

    base = target.select(subject, prediction_time, row_id)

    # Attach observation period bounds (needed to drop censored samples like PLP includeAllOutcomes=FALSE).
    observation_period = ctx.table("observation_period").select(
        "person_id", "observation_period_start_date", "observation_period_end_date"
    )
    op_start = _ensure_timestamp(observation_period.observation_period_start_date)
    op_end = _ensure_timestamp(observation_period.observation_period_end_date)

    joined_op = base.join(
        observation_period,
        [
            base.subject_id == observation_period.person_id,
            op_start <= base.prediction_time,
            op_end >= base.prediction_time,
        ],
        how="left",
    )

    grouped_op = joined_op.group_by(
        joined_op.subject_id, joined_op.prediction_time, joined_op.row_id
    ).aggregate(
        obs_start=op_start.min(),
        obs_end=op_end.max(),
    )

    # Drop rows that don't land in an observation period.
    grouped_op = grouped_op.filter(
        grouped_op.obs_end.notnull() & grouped_op.obs_start.notnull()
    )

    # Apply washout: require index to be >= obs_start + washout.
    if settings.washout_period_days:
        grouped_op = grouped_op.filter(
            grouped_op.prediction_time
            >= grouped_op.obs_start
            + ibis.interval(days=int(settings.washout_period_days))
        )

    tar_start = grouped_op.prediction_time + ibis.interval(
        days=int(settings.risk_window_start_days)
    )
    tar_end_candidate = grouped_op.prediction_time + ibis.interval(
        days=int(settings.risk_window_end_days)
    )
    tar_end = ibis.least(tar_end_candidate, grouped_op.obs_end)

    # firstExposureOnly: keep earliest prediction_time per subject_id (tie-broken by row_id).
    pop = grouped_op.mutate(_tar_start=tar_start, _tar_end=tar_end)
    if settings.first_exposure_only:
        w_first = ibis.window(
            group_by=pop.subject_id, order_by=[pop.prediction_time, pop.row_id]
        )
        ranked = pop.mutate(_rn=ibis.row_number().over(w_first))
        pop = ranked.filter(ranked._rn == 0).drop("_rn")

    # Remove subjects with prior outcomes.
    outcome = outcome_rows.view()
    outcome_subject = outcome[subject_id_col].cast("int64")
    outcome_time = _ensure_timestamp(outcome[outcome_time_col])

    if settings.remove_subjects_with_prior_outcome:
        lookback = ibis.interval(days=int(settings.prior_outcome_lookback_days))
        prior_hits = pop.join(
            outcome,
            [
                pop.subject_id == outcome_subject,
                outcome_time < pop._tar_start,
                outcome_time > (pop.prediction_time - lookback),
            ],
            how="left",
        )
        prior_bad = (
            prior_hits.filter(outcome_time.notnull())
            .select(
                prior_hits.subject_id, prior_hits.prediction_time, prior_hits.row_id
            )
            .distinct()
        )
        pop = pop.anti_join(prior_bad, ["subject_id", "prediction_time", "row_id"])

    # Determine if any outcome exists within TAR.
    hits = pop.join(
        outcome,
        [
            pop.subject_id == outcome_subject,
            outcome_time >= pop._tar_start,
            outcome_time <= pop._tar_end,
        ],
        how="left",
    )
    hit_any = hits.group_by(
        hits.subject_id, hits.prediction_time, hits.row_id
    ).aggregate(
        _has_outcome=outcome_time.count() > 0,
    )
    # Avoid join name collisions by duplicating join keys with distinct names on the RHS.
    hit_any = hit_any.mutate(
        __subject_id=hit_any.subject_id,
        __prediction_time=hit_any.prediction_time,
        __row_id=hit_any.row_id,
    ).drop("subject_id", "prediction_time", "row_id")
    labeled = pop.join(
        hit_any,
        [
            pop.subject_id == hit_any.__subject_id,
            pop.prediction_time == hit_any.__prediction_time,
            pop.row_id == hit_any.__row_id,
        ],
        how="left",
    )
    labeled = labeled.mutate(
        boolean_value=ibis.coalesce(labeled._has_outcome, ibis.literal(False))
    ).drop("_has_outcome", "__subject_id", "__prediction_time", "__row_id")

    # Apply TAR sufficiency filter, optionally keeping positives even if censored.
    if settings.require_time_at_risk:
        min_tar = int(settings.effective_min_time_at_risk_days())
        enough_tar = labeled._tar_end >= labeled._tar_start + ibis.interval(
            days=min_tar
        )
        if settings.include_all_outcomes:
            labeled = labeled.filter(labeled.boolean_value | enough_tar)
        else:
            labeled = labeled.filter(enough_tar)

    return labeled.select(
        labeled.subject_id, labeled.prediction_time, labeled.boolean_value
    )


MEDS_BOOL_LABEL_SCHEMA = {
    "subject_id": pl.Int64,
    "prediction_time": pl.Datetime(time_unit="us"),
    "boolean_value": pl.Boolean,
}


def export_meds_task_labels(
    task_root: str | Path,
    task_name: str,
    labels_df_or_iterable: pl.DataFrame | Iterable[pl.DataFrame],
    *,
    shard_size: int = 250_000,
    overwrite: bool = False,
    task_def: dict[str, Any] | None = None,
) -> Path:
    """
    Write MEDS task label shards under:
      task_root/task_name/labels/part-00000.parquet
    and a sidecar task definition at:
      task_root/task_name/task_def.json
    """
    root = Path(task_root)
    if not task_name:
        raise ValueError("task_name must be non-empty")
    if shard_size <= 0:
        raise ValueError("shard_size must be > 0")

    task_dir = (root / task_name).resolve()
    labels_dir = task_dir / "labels"

    if task_dir.exists():
        if not overwrite:
            raise FileExistsError(f"Task directory already exists: {task_dir}")
        shutil.rmtree(task_dir)

    labels_dir.mkdir(parents=True, exist_ok=True)

    def _iter_frames(
        obj: pl.DataFrame | Iterable[pl.DataFrame],
    ) -> Iterator[pl.DataFrame]:
        if isinstance(obj, pl.DataFrame):
            yield obj
        else:
            yield from obj

    buffer: list[pl.DataFrame] = []
    buffered_rows = 0
    shard_idx = 0

    def _flush(frames: list[pl.DataFrame]) -> None:
        nonlocal shard_idx
        if not frames:
            return
        df = pl.concat(frames, how="vertical")
        df = _coerce_meds_bool_labels(df)
        out = labels_dir / f"part-{shard_idx:05d}.parquet"
        df.write_parquet(out)
        shard_idx += 1

    for df in _iter_frames(labels_df_or_iterable):
        if df.is_empty():
            continue
        buffer.append(df)
        buffered_rows += df.height
        if buffered_rows >= shard_size:
            _flush(buffer)
            buffer = []
            buffered_rows = 0

    _flush(buffer)

    # Write task definition last (so partial writes are easier to detect/clean).
    if task_def is None:
        task_def = {}
    (task_dir / "task_def.json").write_text(
        json.dumps(task_def, indent=2, sort_keys=True) + "\n"
    )

    return task_dir


def _coerce_meds_bool_labels(df: pl.DataFrame) -> pl.DataFrame:
    # Enforce closed schema: keep only MEDS columns.
    cols = ["subject_id", "prediction_time", "boolean_value"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required MEDS label columns: {missing}")
    extra = [c for c in df.columns if c not in cols]
    if extra:
        df = df.select(cols)

    df = df.with_columns(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("prediction_time").cast(pl.Datetime(time_unit="us")),
        pl.col("boolean_value").cast(pl.Boolean),
    )

    # Present columns must be non-null.
    nulls = df.select(pl.all().null_count()).row(0)
    if any(v != 0 for v in nulls):
        raise ValueError(
            f"MEDS label columns must be non-null; null counts={dict(zip(df.columns, nulls))}"
        )

    return df.select(
        pl.col("subject_id"), pl.col("prediction_time"), pl.col("boolean_value")
    )
