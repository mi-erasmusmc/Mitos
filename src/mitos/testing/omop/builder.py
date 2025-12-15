from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

import polars as pl

import ibis

from .schema import OMOP_SCHEMAS


def _default_for_type(dtype: str):
    # Use None for most optional columns; ids and dates are set by callers.
    return None


def _polars_dtype(dtype: str):
    dtype = dtype.lower()
    if dtype in {"int64", "int"}:
        return pl.Int64
    if dtype in {"float64", "float", "double"}:
        return pl.Float64
    if dtype == "date":
        return pl.Date
    if dtype == "timestamp":
        return pl.Datetime
    if dtype in {"string", "str", "varchar"}:
        return pl.Utf8
    raise ValueError(f"Unsupported dtype in OMOP_SCHEMAS: {dtype!r}")


@dataclass
class OmopBuilder:
    schema: str = "main"
    _rows: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    _counters: dict[str, int] = field(default_factory=dict)

    def ensure_tables(self, con: ibis.BaseBackend, names: Iterable[str]) -> None:
        for name in names:
            self._ensure_table(con, name)

    def _ensure_table(self, con: ibis.BaseBackend, name: str) -> None:
        cols = OMOP_SCHEMAS.get(name)
        if cols is None:
            raise KeyError(f"Unknown OMOP table in test schema registry: {name}")
        try:
            # If it exists, do nothing.
            con.table(name, database=self.schema)
            return
        except Exception:
            pass

        # Create empty table with schema only.
        con.create_table(
            name,
            schema=cols,
            database=self.schema,
            overwrite=True,
        )

    def _next_id(self, key: str) -> int:
        self._counters[key] = self._counters.get(key, 0) + 1
        return self._counters[key]

    def add_person(
        self,
        *,
        person_id: int,
        gender_concept_id: int = 0,
        year_of_birth: int = 1970,
        month_of_birth: int = 1,
        day_of_birth: int = 1,
        race_concept_id: int = 0,
        ethnicity_concept_id: int = 0,
    ) -> None:
        self._rows["person"].append(
            {
                "person_id": int(person_id),
                "gender_concept_id": int(gender_concept_id),
                "year_of_birth": int(year_of_birth),
                "month_of_birth": int(month_of_birth),
                "day_of_birth": int(day_of_birth),
                "race_concept_id": int(race_concept_id),
                "ethnicity_concept_id": int(ethnicity_concept_id),
            }
        )

    def add_observation_period(
        self,
        *,
        person_id: int,
        start_date: date,
        end_date: date,
        observation_period_id: int | None = None,
        period_type_concept_id: int = 0,
    ) -> None:
        self._rows["observation_period"].append(
            {
                "observation_period_id": int(observation_period_id or self._next_id("observation_period_id")),
                "person_id": int(person_id),
                "observation_period_start_date": start_date,
                "observation_period_end_date": end_date,
                "period_type_concept_id": int(period_type_concept_id),
            }
        )

    def add_visit_occurrence(
        self,
        *,
        person_id: int,
        visit_start_date: date,
        visit_end_date: date | None = None,
        visit_occurrence_id: int | None = None,
        visit_concept_id: int = 0,
        visit_type_concept_id: int = 0,
        visit_source_concept_id: int = 0,
    ) -> int:
        visit_id = int(visit_occurrence_id or self._next_id("visit_occurrence_id"))
        self._rows["visit_occurrence"].append(
            {
                "visit_occurrence_id": visit_id,
                "person_id": int(person_id),
                "visit_concept_id": int(visit_concept_id),
                "visit_start_date": visit_start_date,
                "visit_end_date": visit_end_date or visit_start_date,
                "visit_type_concept_id": int(visit_type_concept_id),
                "visit_source_concept_id": int(visit_source_concept_id),
            }
        )
        return visit_id

    def add_condition_occurrence(
        self,
        *,
        person_id: int,
        condition_concept_id: int,
        condition_start_date: date,
        condition_end_date: date | None = None,
        condition_occurrence_id: int | None = None,
        condition_type_concept_id: int = 0,
        condition_status_concept_id: int = 0,
        condition_source_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        occ_id = int(condition_occurrence_id or self._next_id("condition_occurrence_id"))
        self._rows["condition_occurrence"].append(
            {
                "condition_occurrence_id": occ_id,
                "person_id": int(person_id),
                "condition_concept_id": int(condition_concept_id),
                "condition_start_date": condition_start_date,
                "condition_end_date": condition_end_date or condition_start_date,
                "condition_type_concept_id": int(condition_type_concept_id),
                "condition_status_concept_id": int(condition_status_concept_id),
                "condition_source_concept_id": int(condition_source_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return occ_id

    def add_measurement(
        self,
        *,
        person_id: int,
        measurement_concept_id: int,
        measurement_date: date,
        measurement_id: int | None = None,
        measurement_datetime: datetime | None = None,
        measurement_type_concept_id: int = 0,
        operator_concept_id: int = 0,
        value_as_number: float | None = None,
        value_as_concept_id: int = 0,
        unit_concept_id: int = 0,
        range_low: float | None = None,
        range_high: float | None = None,
        visit_occurrence_id: int | None = None,
        measurement_source_concept_id: int = 0,
    ) -> int:
        meas_id = int(measurement_id or self._next_id("measurement_id"))
        self._rows["measurement"].append(
            {
                "measurement_id": meas_id,
                "person_id": int(person_id),
                "measurement_concept_id": int(measurement_concept_id),
                "measurement_date": measurement_date,
                "measurement_datetime": measurement_datetime,
                "measurement_type_concept_id": int(measurement_type_concept_id),
                "operator_concept_id": int(operator_concept_id),
                "value_as_number": float(value_as_number) if value_as_number is not None else None,
                "value_as_concept_id": int(value_as_concept_id),
                "unit_concept_id": int(unit_concept_id),
                "range_low": float(range_low) if range_low is not None else None,
                "range_high": float(range_high) if range_high is not None else None,
                "visit_occurrence_id": int(visit_occurrence_id or 0),
                "measurement_source_concept_id": int(measurement_source_concept_id),
            }
        )
        return meas_id

    def materialize(self, con: ibis.BaseBackend) -> None:
        # Create all tables touched by rows, plus ensure required tables exist as empty.
        for name in sorted(OMOP_SCHEMAS.keys()):
            self._ensure_table(con, name)

        for name, rows in self._rows.items():
            cols = OMOP_SCHEMAS[name]
            schema = {col: _polars_dtype(dtype) for col, dtype in cols.items()}
            # Ensure every column exists; default missing fields to None.
            normalized: list[dict[str, Any]] = []
            for row in rows:
                out = {col: row.get(col, _default_for_type(dtype)) for col, dtype in cols.items()}
                normalized.append(out)

            if normalized:
                df = pl.DataFrame(normalized, schema=schema)
            else:
                df = pl.DataFrame(schema=schema)
            con.create_table(
                name,
                obj=df,
                database=self.schema,
                overwrite=True,
            )
