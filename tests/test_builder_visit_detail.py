from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import VisitDetail

import ibis_cohort.builders.visit_detail  # noqa: F401


def make_context(conn):
    codesets = ibis.memtable(
        {
            "codeset_id": [1, 2, 3, 4, 5, 6, 7],
            "concept_id": [101, 401, 301, 601, 501, 701, 8507],
        }
    )
    return BuildContext(conn, CohortBuildOptions(), codesets)


def test_visit_detail_filters_concepts_and_locations():
    conn = ibis.duckdb.connect(database=":memory:")

    visit_detail_df = pl.DataFrame(
        {
            "visit_detail_id": [1, 2],
            "person_id": [1, 1],
            "visit_detail_concept_id": [101, 999],
            "visit_occurrence_id": [10, 11],
            "provider_id": [100, 101],
            "care_site_id": [200, 201],
            "visit_detail_start_date": [datetime(2020, 1, 1), datetime(2020, 3, 1)],
            "visit_detail_end_date": [datetime(2020, 1, 10), datetime(2020, 3, 2)],
            "visit_detail_source_concept_id": [301, 999],
            "visit_detail_type_concept_id": [401, 999],
        }
    )
    conn.create_table("visit_detail", visit_detail_df, overwrite=True)

    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("person", person_df, overwrite=True)

    provider_df = pl.DataFrame({"provider_id": [100, 101], "specialty_concept_id": [601, 999]})
    conn.create_table("provider", provider_df, overwrite=True)

    care_site_df = pl.DataFrame(
        {"care_site_id": [200, 201], "place_of_service_concept_id": [501, 999], "location_id": [3000, 3001]}
    )
    conn.create_table("care_site", care_site_df, overwrite=True)

    location_history_df = pl.DataFrame(
        {
            "location_history_id": [1, 2],
            "location_id": [3000, 3001],
            "entity_id": [200, 201],
            "domain_id": ["CARE_SITE", "CARE_SITE"],
            "start_date": pl.Series("start_date", [datetime(2019, 1, 1), datetime(2019, 1, 1)], dtype=pl.Datetime),
            "end_date": pl.Series("end_date", [None, None], dtype=pl.Datetime),
        }
    )
    conn.create_table("location_history", location_history_df, overwrite=True)

    location_df = pl.DataFrame({"location_id": [3000, 3001], "region_concept_id": [701, 999]})
    conn.create_table("location", location_df, overwrite=True)

    ctx = make_context(conn)
    criteria = VisitDetail(
        **{
            "CodesetId": 1,
            "VisitDetailTypeCS": {"CodesetId": 2},
            "VisitDetailSourceConcept": 3,
            "VisitDetailLength": {"Value": 5, "Op": "gte"},
            "GenderCS": {"CodesetId": 7},
            "ProviderSpecialtyCS": {"CodesetId": 4},
            "PlaceOfServiceCS": {"CodesetId": 5},
            "PlaceOfServiceLocation": 6,
            "First": True,
        }
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
    assert result["start_date"][0].date().isoformat() == "2020-01-01"
