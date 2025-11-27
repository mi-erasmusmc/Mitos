from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import DoseEra

import ibis_cohort.builders.dose_era  # noqa: F401


def make_context(conn, concept_ids):
    codesets = ibis.memtable(
        {
            "codeset_id": [1] * len(concept_ids),
            "concept_id": concept_ids,
        }
    )
    return BuildContext(conn, CohortBuildOptions(), codesets)


def test_dose_era_filters_and_first_flag():
    conn = ibis.duckdb.connect(database=":memory:")
    dose_era_df = pl.DataFrame(
        {
            "dose_era_id": [1, 2, 3],
            "person_id": [1, 1, 1],
            "drug_concept_id": [2001, 2001, 2001],
            "dose_era_start_date": [
                datetime(2020, 1, 1),
                datetime(2020, 2, 1),
                datetime(2020, 3, 1),
            ],
            "dose_era_end_date": [
                datetime(2020, 1, 10),
                datetime(2020, 2, 10),
                datetime(2020, 3, 10),
            ],
            "unit_concept_id": [3001, 3001, 3001],
            "dose_value": [5.0, 15.0, 25.0],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("dose_era", dose_era_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn, [2001])
    criteria = DoseEra(
        **{
            "CodesetId": 1,
            "Unit": [{"CONCEPT_ID": 3001}],
            "DoseValue": {"Value": 10, "Op": "gte"},
            "First": True,
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [2], "Dose value filter should retain second era and first flag keeps earliest"
