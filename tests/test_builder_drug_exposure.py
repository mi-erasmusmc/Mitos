from datetime import datetime
import uuid

import ibis
import polars as pl

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import DrugExposure


def make_context(conn, codesets=None):
    if codesets is None:
        codesets = ibis.memtable({"codeset_id": [1], "concept_id": [2001]})
    name = f"codesets_{uuid.uuid4().hex}"
    conn.create_table(name, codesets, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


def test_drug_first_requires_initial_event_to_satisfy_date_filter():
    conn = ibis.duckdb.connect(database=":memory:")
    drug_df = pl.DataFrame(
        {
            "drug_exposure_id": [1, 2],
            "person_id": [1, 1],
            "drug_concept_id": [2001, 2001],
            "drug_exposure_start_date": [datetime(2009, 3, 1), datetime(2011, 6, 1)],
            "drug_exposure_end_date": [datetime(2009, 3, 2), datetime(2011, 6, 2)],
            "drug_type_concept_id": [0, 0],
            "visit_occurrence_id": pl.Series([None, None], dtype=pl.Int64),
        }
    )
    conn.create_table("drug_exposure", drug_df, overwrite=True)

    ctx = make_context(conn)
    criteria = DrugExposure(
        **{
            "CodesetId": 1,
            "First": True,
            "OccurrenceStartDate": {"Value": "2010-01-01", "Op": "gte"},
        }
    )

    events = build_events(criteria, ctx)
    assert events.count().execute() == 0, (
        "First exposure occurs before allowed window, so later exposures should be excluded"
    )
