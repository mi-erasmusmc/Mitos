from datetime import datetime

import polars as pl
import ibis

from ibis_cohort.build_context import BuildContext, CohortBuildOptions
from ibis_cohort.builders.registry import build_events
from ibis_cohort.tables import ProcedureOccurrence

import ibis_cohort.builders.procedure_occurrence  # noqa: F401


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [601]})
    return BuildContext(conn, CohortBuildOptions(), codeset_expr)


def test_procedure_occurrence_builder_filters_codeset():
    conn = ibis.duckdb.connect(database=":memory:")
    procedure_df = pl.DataFrame(
        {
            "procedure_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "procedure_concept_id": [601, 602],
            "procedure_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "procedure_type_concept_id": [0, 0],
            "visit_occurrence_id": [1, 1],
        }
    )
    person_df = pl.DataFrame({"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]})
    conn.create_table("procedure_occurrence", procedure_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = ProcedureOccurrence(**{"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]
