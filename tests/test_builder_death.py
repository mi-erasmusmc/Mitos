from datetime import datetime
import uuid

import polars as pl
import ibis

from mitos.build_context import BuildContext, CohortBuildOptions
from mitos.builders.registry import build_events
from mitos.tables import Death


def make_context(conn):
    codeset_expr = ibis.memtable({"codeset_id": [1], "concept_id": [701]})
    name = f"codesets_{uuid.uuid4().hex}"
    conn.create_table(name, codeset_expr, temp=True)
    return BuildContext(conn, CohortBuildOptions(), conn.table(name))


def test_death_builder_generates_events():
    conn = ibis.duckdb.connect(database=":memory:")
    death_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "death_date": [datetime(2020, 1, 1), datetime(2020, 2, 1)],
            "death_type_concept_id": [0, 0],
            "cause_concept_id": [701, 702],
        }
    )
    person_df = pl.DataFrame(
        {
            "person_id": [1, 2],
            "year_of_birth": [1980, 1970],
            "gender_concept_id": [8507, 8532],
        }
    )
    conn.create_table("death", death_df, overwrite=True)
    conn.create_table("person", person_df, overwrite=True)

    ctx = make_context(conn)
    criteria = Death.model_validate({"CodesetId": 1})

    events = build_events(criteria, ctx)
    result = events.to_polars()

    assert result["person_id"].to_list() == [1]
