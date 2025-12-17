from __future__ import annotations

from datetime import date, datetime

import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.measurement import build_measurement
from mitos.criteria import NumericRange
from mitos.tables import Measurement


def test_measurement_range_high_ratio_is_applied():
    con = ibis.duckdb.connect(database=":memory:")
    schema = "main"

    con.create_table(
        "measurement",
        database=schema,
        overwrite=True,
        obj=ibis.memtable(
            [
                # ratio = 60/10 = 6 (passes gt 5)
                {
                    "measurement_id": 1,
                    "person_id": 1,
                    "measurement_concept_id": 123,
                    "measurement_date": date(2020, 1, 1),
                    "measurement_datetime": datetime(2020, 1, 1),
                    "measurement_type_concept_id": 0,
                    "operator_concept_id": 0,
                    "value_as_number": 60.0,
                    "value_as_concept_id": 0,
                    "unit_concept_id": 0,
                    "range_low": 0.0,
                    "range_high": 10.0,
                    "visit_occurrence_id": 0,
                    "measurement_source_concept_id": 0,
                },
                # ratio = 40/10 = 4 (fails gt 5)
                {
                    "measurement_id": 2,
                    "person_id": 2,
                    "measurement_concept_id": 123,
                    "measurement_date": date(2020, 1, 1),
                    "measurement_datetime": datetime(2020, 1, 1),
                    "measurement_type_concept_id": 0,
                    "operator_concept_id": 0,
                    "value_as_number": 40.0,
                    "value_as_concept_id": 0,
                    "unit_concept_id": 0,
                    "range_low": 0.0,
                    "range_high": 10.0,
                    "visit_occurrence_id": 0,
                    "measurement_source_concept_id": 0,
                },
            ]
        ),
    )

    con.create_table(
        "codesets",
        database=schema,
        overwrite=True,
        obj=ibis.memtable([{"codeset_id": 1, "concept_id": 123}]),
    )
    options = CohortBuildOptions(cdm_schema=schema, vocabulary_schema=schema, backend="duckdb")
    ctx = BuildContext(con, options, con.table("codesets", database=schema))
    try:
        criteria = Measurement.model_validate(
            {"CodesetId": 1, "RangeHighRatio": {"Value": 5, "Op": "gt"}}
        )
        out = build_measurement(criteria, ctx)
        df = out.execute()
        assert set(int(v) for v in df["person_id"].tolist()) == {1}
    finally:
        ctx.close()
