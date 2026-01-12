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

def _duckdb_sql_type(dtype: str) -> str:
    dtype = dtype.lower()
    if dtype in {"int64", "int"}:
        return "BIGINT"
    if dtype in {"float64", "float", "double"}:
        return "DOUBLE"
    if dtype == "date":
        return "DATE"
    if dtype == "timestamp":
        return "TIMESTAMP"
    if dtype in {"string", "str", "varchar"}:
        return "VARCHAR"
    raise ValueError(f"Unsupported dtype in OMOP_SCHEMAS: {dtype!r}")

def _is_duckdb_backend(con: ibis.BaseBackend) -> bool:
    # `ibis.duckdb.connect` returns a backend with `.con` = duckdb.DuckDBPyConnection.
    return hasattr(con, "con") and con.__class__.__module__.endswith("ibis.backends.duckdb.__init__")

def _quote_ident(ident: str) -> str:
    # Keep it simple: double-quote and escape embedded quotes.
    return '"' + ident.replace('"', '""') + '"'

def _qualified(schema: str, name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


@dataclass
class OmopBuilder:
    schema: str = "main"
    _rows: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    _counters: dict[str, int] = field(default_factory=dict)

    def ensure_tables(self, con: ibis.BaseBackend, names: Iterable[str]) -> None:
        for name in names:
            self._ensure_table(con, name)

    def _ensure_table(self, con: ibis.BaseBackend, name: str, *, assume_missing: bool = False) -> None:
        cols = OMOP_SCHEMAS.get(name)
        if cols is None:
            raise KeyError(f"Unknown OMOP table in test schema registry: {name}")
        if not assume_missing:
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

    def add_provider(
        self,
        *,
        provider_id: int,
        specialty_concept_id: int = 0,
    ) -> None:
        self._rows["provider"].append(
            {
                "provider_id": int(provider_id),
                "specialty_concept_id": int(specialty_concept_id),
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
        provider_id: int = 0,
        care_site_id: int = 0,
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
                "provider_id": int(provider_id),
                "care_site_id": int(care_site_id),
            }
        )
        return visit_id

    def add_visit_detail(
        self,
        *,
        person_id: int,
        visit_detail_concept_id: int,
        visit_detail_start_date: date,
        visit_detail_end_date: date | None = None,
        visit_detail_id: int | None = None,
        visit_detail_type_concept_id: int = 0,
        visit_detail_source_concept_id: int = 0,
        provider_id: int = 0,
        care_site_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        detail_id = int(visit_detail_id or self._next_id("visit_detail_id"))
        self._rows["visit_detail"].append(
            {
                "visit_detail_id": detail_id,
                "person_id": int(person_id),
                "visit_detail_concept_id": int(visit_detail_concept_id),
                "visit_detail_start_date": visit_detail_start_date,
                "visit_detail_end_date": visit_detail_end_date or visit_detail_start_date,
                "visit_detail_type_concept_id": int(visit_detail_type_concept_id),
                "visit_detail_source_concept_id": int(visit_detail_source_concept_id),
                "provider_id": int(provider_id),
                "care_site_id": int(care_site_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return detail_id

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

    def add_condition_era(
        self,
        *,
        person_id: int,
        condition_concept_id: int,
        condition_era_start_date: date,
        condition_era_end_date: date,
        condition_era_id: int | None = None,
        condition_occurrence_count: int = 1,
    ) -> int:
        era_id = int(condition_era_id or self._next_id("condition_era_id"))
        self._rows["condition_era"].append(
            {
                "condition_era_id": era_id,
                "person_id": int(person_id),
                "condition_concept_id": int(condition_concept_id),
                "condition_era_start_date": condition_era_start_date,
                "condition_era_end_date": condition_era_end_date,
                "condition_occurrence_count": int(condition_occurrence_count),
            }
        )
        return era_id

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
        provider_id: int | None = None,
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
                "provider_id": int(provider_id or 0),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
                "measurement_source_concept_id": int(measurement_source_concept_id),
            }
        )
        return meas_id

    def add_drug_exposure(
        self,
        *,
        person_id: int,
        drug_concept_id: int,
        drug_exposure_start_date: date,
        drug_exposure_end_date: date | None = None,
        drug_exposure_id: int | None = None,
        days_supply: int = 0,
        quantity: float | None = None,
        refills: int = 0,
        drug_type_concept_id: int = 0,
        route_concept_id: int = 0,
        dose_unit_concept_id: int = 0,
        lot_number: str | None = None,
        stop_reason: str | None = None,
        provider_id: int = 0,
        drug_source_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        exp_id = int(drug_exposure_id or self._next_id("drug_exposure_id"))
        self._rows["drug_exposure"].append(
            {
                "drug_exposure_id": exp_id,
                "person_id": int(person_id),
                "drug_concept_id": int(drug_concept_id),
                "drug_exposure_start_date": drug_exposure_start_date,
                "drug_exposure_end_date": drug_exposure_end_date or drug_exposure_start_date,
                "days_supply": int(days_supply),
                "quantity": float(quantity) if quantity is not None else None,
                "refills": int(refills),
                "drug_type_concept_id": int(drug_type_concept_id),
                "route_concept_id": int(route_concept_id),
                "dose_unit_concept_id": int(dose_unit_concept_id),
                "lot_number": lot_number,
                "stop_reason": stop_reason,
                "provider_id": int(provider_id),
                "drug_source_concept_id": int(drug_source_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return exp_id

    def add_drug_era(
        self,
        *,
        person_id: int,
        drug_concept_id: int,
        drug_era_start_date: date,
        drug_era_end_date: date,
        drug_era_id: int | None = None,
        drug_exposure_count: int = 1,
        gap_days: int = 0,
    ) -> int:
        era_id = int(drug_era_id or self._next_id("drug_era_id"))
        self._rows["drug_era"].append(
            {
                "drug_era_id": era_id,
                "person_id": int(person_id),
                "drug_concept_id": int(drug_concept_id),
                "drug_era_start_date": drug_era_start_date,
                "drug_era_end_date": drug_era_end_date,
                "drug_exposure_count": int(drug_exposure_count),
                "gap_days": int(gap_days),
            }
        )
        return era_id

    def add_dose_era(
        self,
        *,
        person_id: int,
        drug_concept_id: int,
        dose_era_start_date: date,
        dose_era_end_date: date,
        dose_era_id: int | None = None,
        unit_concept_id: int = 0,
        dose_value: float | None = None,
    ) -> int:
        era_id = int(dose_era_id or self._next_id("dose_era_id"))
        self._rows["dose_era"].append(
            {
                "dose_era_id": era_id,
                "person_id": int(person_id),
                "drug_concept_id": int(drug_concept_id),
                "unit_concept_id": int(unit_concept_id),
                "dose_value": float(dose_value) if dose_value is not None else None,
                "dose_era_start_date": dose_era_start_date,
                "dose_era_end_date": dose_era_end_date,
            }
        )
        return era_id

    def add_device_exposure(
        self,
        *,
        person_id: int,
        device_concept_id: int,
        device_exposure_start_date: date,
        device_exposure_end_date: date | None = None,
        device_exposure_id: int | None = None,
        device_type_concept_id: int = 0,
        quantity: float | None = None,
        unique_device_id: str | None = None,
        provider_id: int = 0,
        device_source_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        exp_id = int(device_exposure_id or self._next_id("device_exposure_id"))
        self._rows["device_exposure"].append(
            {
                "device_exposure_id": exp_id,
                "person_id": int(person_id),
                "device_concept_id": int(device_concept_id),
                "device_exposure_start_date": device_exposure_start_date,
                "device_exposure_end_date": device_exposure_end_date or device_exposure_start_date,
                "device_type_concept_id": int(device_type_concept_id),
                "quantity": float(quantity) if quantity is not None else None,
                "unique_device_id": unique_device_id,
                "provider_id": int(provider_id),
                "device_source_concept_id": int(device_source_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return exp_id

    def add_procedure_occurrence(
        self,
        *,
        person_id: int,
        procedure_concept_id: int,
        procedure_date: date,
        procedure_end_date: date | None = None,
        procedure_occurrence_id: int | None = None,
        procedure_type_concept_id: int = 0,
        modifier_concept_id: int = 0,
        quantity: float | None = None,
        provider_id: int = 0,
        procedure_source_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        occ_id = int(procedure_occurrence_id or self._next_id("procedure_occurrence_id"))
        self._rows["procedure_occurrence"].append(
            {
                "procedure_occurrence_id": occ_id,
                "person_id": int(person_id),
                "procedure_concept_id": int(procedure_concept_id),
                "procedure_date": procedure_date,
                "procedure_end_date": procedure_end_date or procedure_date,
                "procedure_type_concept_id": int(procedure_type_concept_id),
                "modifier_concept_id": int(modifier_concept_id),
                "quantity": float(quantity) if quantity is not None else None,
                "provider_id": int(provider_id),
                "procedure_source_concept_id": int(procedure_source_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return occ_id

    def add_observation(
        self,
        *,
        person_id: int,
        observation_concept_id: int,
        observation_date: date,
        observation_id: int | None = None,
        observation_type_concept_id: int = 0,
        value_as_number: float | None = None,
        value_as_string: str | None = None,
        value_as_concept_id: int = 0,
        unit_concept_id: int = 0,
        observation_source_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        obs_id = int(observation_id or self._next_id("observation_id"))
        self._rows["observation"].append(
            {
                "observation_id": obs_id,
                "person_id": int(person_id),
                "observation_concept_id": int(observation_concept_id),
                "observation_date": observation_date,
                "value_as_number": float(value_as_number) if value_as_number is not None else None,
                "value_as_string": value_as_string,
                "value_as_concept_id": int(value_as_concept_id),
                "unit_concept_id": int(unit_concept_id),
                "observation_type_concept_id": int(observation_type_concept_id),
                "observation_source_concept_id": int(observation_source_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return obs_id

    def add_specimen(
        self,
        *,
        person_id: int,
        specimen_concept_id: int,
        specimen_date: date,
        specimen_id: int | None = None,
        specimen_type_concept_id: int = 0,
        visit_occurrence_id: int | None = None,
    ) -> int:
        specimen_id_out = int(specimen_id or self._next_id("specimen_id"))
        self._rows["specimen"].append(
            {
                "specimen_id": specimen_id_out,
                "person_id": int(person_id),
                "specimen_concept_id": int(specimen_concept_id),
                "specimen_date": specimen_date,
                "specimen_type_concept_id": int(specimen_type_concept_id),
                "visit_occurrence_id": int(visit_occurrence_id or 0),
            }
        )
        return specimen_id_out

    def add_death(
        self,
        *,
        person_id: int,
        death_date: date,
        cause_concept_id: int = 0,
        death_type_concept_id: int = 0,
    ) -> None:
        self._rows["death"].append(
            {
                "person_id": int(person_id),
                "cause_concept_id": int(cause_concept_id),
                "death_date": death_date,
                "death_type_concept_id": int(death_type_concept_id),
            }
        )

    def materialize(
        self,
        con: ibis.BaseBackend,
        *,
        ensure_all_tables: bool = True,
        fast: bool = False,
    ) -> None:
        """
        Materialize accumulated rows into DuckDB.

        By default this creates the full OMOP schema (all tables) to avoid missing-table errors.
        For smaller unit-test scenarios, pass `ensure_all_tables=False` to only create tables
        referenced by this builder (plus `person` and `observation_period`).
        """
        if fast and _is_duckdb_backend(con):
            self._materialize_duckdb_fast(con, ensure_all_tables=ensure_all_tables)
            return

        if ensure_all_tables:
            for name in sorted(OMOP_SCHEMAS.keys()):
                self._ensure_table(con, name)
        else:
            required = set(self._rows.keys()) | {"person", "observation_period"}
            for name in sorted(required):
                self._ensure_table(con, name, assume_missing=True)

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

    def _materialize_duckdb_fast(self, con: ibis.BaseBackend, *, ensure_all_tables: bool) -> None:
        """
        Fast-path table creation/loading for DuckDB.

        Uses raw SQL for DDL and duckdb.register() + INSERT for data to avoid Ibis schema
        introspection and most SQLGlot compilation overhead.
        """
        duck = con.con

        if ensure_all_tables:
            required = set(OMOP_SCHEMAS.keys())
        else:
            required = set(self._rows.keys()) | {"person", "observation_period"}

        for name in sorted(required):
            cols = OMOP_SCHEMAS.get(name)
            if cols is None:
                raise KeyError(f"Unknown OMOP table in test schema registry: {name}")
            col_sql = ", ".join(f"{_quote_ident(col)} {_duckdb_sql_type(dtype)}" for col, dtype in cols.items())
            con.raw_sql(f"DROP TABLE IF EXISTS {_qualified(self.schema, name)}")
            con.raw_sql(f"CREATE TABLE {_qualified(self.schema, name)} ({col_sql})")

        for name, rows in self._rows.items():
            cols = OMOP_SCHEMAS[name]
            schema = {col: _polars_dtype(dtype) for col, dtype in cols.items()}
            normalized: list[dict[str, Any]] = []
            for row in rows:
                out = {col: row.get(col, _default_for_type(dtype)) for col, dtype in cols.items()}
                normalized.append(out)

            if normalized:
                df = pl.DataFrame(normalized, schema=schema)
                tmp = f"__mitos_tmp_{name}__"
                duck.register(tmp, df.to_arrow())
                try:
                    con.raw_sql(
                        f"INSERT INTO {_qualified(self.schema, name)} SELECT * FROM {_quote_ident(tmp)}"
                    )
                finally:
                    duck.unregister(tmp)
