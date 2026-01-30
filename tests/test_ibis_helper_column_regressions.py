from __future__ import annotations

import ibis
import polars as pl

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.groups import _attach_count_columns
from mitos.criteria import CriteriaColumn


class _CriteriaModel:
    def snake_case_class_name(self) -> str:
        return "measurement"

    def get_primary_key_column(self) -> str:
        return "measurement_id"


def test_attach_count_columns_does_not_leak_join_keys_into_materialize():
    conn = ibis.duckdb.connect(database=":memory:")
    conn.create_table(
        "measurement",
        pl.DataFrame(
            {
                "measurement_id": [1, 2],
                "measurement_source_concept_id": [10, 20],
            }
        ),
        overwrite=True,
    )

    events = ibis.memtable(
        [
            {"person_id": 1, "event_id": 1, "start_date": "2020-01-01"},
            {"person_id": 1, "event_id": 2, "start_date": "2020-01-02"},
        ],
        schema={"person_id": "int64", "event_id": "int64", "start_date": "string"},
    )

    ctx = BuildContext(
        conn,
        CohortBuildOptions(),
        ibis.memtable([], schema={"codeset_id": "int64", "concept_id": "int64"}),
    )

    augmented = _attach_count_columns(
        events,
        _CriteriaModel(),
        ctx,
        count_column_name="_corr_domain_source_concept_id",
        count_column_enum=CriteriaColumn.DOMAIN_SOURCE_CONCEPT,
    )

    # Regression guard: this path used to introduce helper join-key columns that could
    # leak into CREATE TABLE AS / INSERT statements on DuckDB.
    ctx.materialize(augmented, label="attach_count_columns", temp=True, analyze=False)
