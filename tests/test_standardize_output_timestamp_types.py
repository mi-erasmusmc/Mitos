from __future__ import annotations

from datetime import datetime

import ibis

from mitos.builders.common import standardize_output


def test_standardize_output_casts_start_end_to_timestamp():
    t = ibis.memtable(
        [
            {
                "person_id": 1,
                "pk": 10,
                "start_dt": datetime(2020, 1, 2, 3, 4, 5),
            }
        ],
        schema={"person_id": "int64", "pk": "int64", "start_dt": "date"},
    )
    out = standardize_output(
        t, primary_key="pk", start_column="start_dt", end_column="start_dt"
    )
    assert str(out.schema()["start_date"]) == "timestamp"
    assert str(out.schema()["end_date"]) == "timestamp"
