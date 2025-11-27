from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import Specimen

import ibis_cohort.builders.specimen  # noqa: F401


def make_context(conn, concept_ids):
    codesets = ibis.memtable(
        {
            "codeset_id": [1] * len(concept_ids),
            "concept_id": concept_ids,
        }
    )
    return BuildContext(conn, CohortBuildOptions(), codesets)


def test_specimen_applies_text_and_codeset_filters():
    conn = ibis.duckdb.connect(database=":memory:")
    specimen_df = pl.DataFrame(
        {
            "specimen_id": [1, 2],
            "person_id": [1, 1],
            "specimen_concept_id": [5001, 5001],
            "specimen_date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "specimen_type_concept_id": [6001, 6001],
            "quantity": [1.0, 2.0],
            "unit_concept_id": [7001, 7001],
            "anatomic_site_concept_id": [8001, 8001],
            "disease_status_concept_id": [9001, 9001],
            "specimen_source_id": ["ABC-123", "XYZ-999"],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})

    conn.create_table("specimen", specimen_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn, [5001])
    criteria = Specimen(
        **{
            "CodesetId": 1,
            "SourceId": {"Text": "ABC", "Op": "startsWith"},
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
