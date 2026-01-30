from datetime import datetime
import uuid

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import ProcedureOccurrence


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [601]})
    name = f"codeset_{uuid.uuid4().hex}"
    conn.create_table(name, codeset_expr, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


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
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("procedure_occurrence", procedure_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = ProcedureOccurrence.model_validate({"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["event_id"].to_list() == [1]


def test_procedure_occurrence_filters_source_concept_codeset():
    conn = ibis.duckdb.connect(database=":memory:")
    procedure_df = pl.DataFrame(
        {
            "procedure_occurrence_id": [1, 2],
            "person_id": [1, 1],
            "procedure_concept_id": [601, 601],
            "procedure_source_concept_id": [701, 702],
            "procedure_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "procedure_type_concept_id": [0, 0],
            "visit_occurrence_id": [1, 1],
        }
    )
    person_df = pl.DataFrame(
        {"person_id": [1], "year_of_birth": [1980], "gender_concept_id": [8507]}
    )
    conn.create_table("procedure_occurrence", procedure_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    codeset_df = pl.DataFrame({"codeset_id": [1, 2], "concept_id": [601, 701]})
    ctx = BuildContext(
        conn,
        CohortBuildOptions(),
        conn.create_table("codesets_source_test", codeset_df, overwrite=True),
    )
    criteria = ProcedureOccurrence.model_validate(
        {"CodesetId": 1, "ProcedureSourceConcept": 2}
    )

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result.height == 1
    assert result["event_id"].to_list() == [1]
