from __future__ import annotations

import ibis
import polars as pl

from mitos.build_context import BuildContext, CohortBuildOptions, CodesetResource
from mitos.builders.common import apply_codeset_filter
from mitos.builders.groups import _combine_threshold
from mitos.builders.post_processing import apply_inclusion_rules
from mitos.ibis_compat import table_from_literal_list
from mitos.cohort_expression import InclusionRule
from mitos.criteria import CriteriaGroup


def test_table_from_literal_list_empty_compiles_for_postgres():
    expr = table_from_literal_list([], column_name="concept_id", element_type="int64")
    sql = expr.to_sql(dialect="postgres").upper()
    assert "ARRAY[]" not in sql


def test_threshold_aggregation_does_not_cast_boolean_to_bigint_in_postgres_sql():
    masks = [ibis.literal(True), ibis.literal(False), ibis.literal(True)]
    expr = _combine_threshold(masks, 2, at_least=True)
    sql = expr.to_sql(dialect="postgres").upper()
    assert "CAST(TRUE AS BIGINT)" not in sql
    assert "CAST(FALSE AS BIGINT)" not in sql


def test_apply_codeset_filter_can_be_applied_twice_without_ambiguous_field_errors():
    con = ibis.duckdb.connect(database=":memory:")
    con.create_table(
        "observation",
        pl.DataFrame(
            {
                "observation_id": [1, 2],
                "person_id": [1, 1],
                "observation_concept_id": [10, 99],
                "observation_source_concept_id": [20, 99],
            }
        ),
        overwrite=True,
    )
    con.create_table(
        "_codesets",
        pl.DataFrame(
            {
                "codeset_id": [1, 1],
                "concept_id": [10, 20],
            }
        ),
        overwrite=True,
    )

    options = CohortBuildOptions(
        cdm_schema="main", vocabulary_schema="main", backend="duckdb"
    )
    ctx = BuildContext(con, options, CodesetResource(table=con.table("_codesets")))
    try:
        t = ctx.table("observation")
        t = apply_codeset_filter(t, "observation_concept_id", 1, ctx)
        t = apply_codeset_filter(t, "observation_source_concept_id", 1, ctx)
        assert int(t.count().execute()) == 1
    finally:
        ctx.close()


def test_inclusion_rule_mask_uses_integer_bitops_in_postgres_sql():
    con = ibis.duckdb.connect(database=":memory:")
    con.create_table(
        "events",
        pl.DataFrame(
            {
                "person_id": [1],
                "event_id": [1],
            }
        ),
        overwrite=True,
    )
    options = CohortBuildOptions(
        cdm_schema="main", vocabulary_schema="main", backend="duckdb"
    )
    ctx = BuildContext(
        con,
        options,
        CodesetResource(
            table=table_from_literal_list([], column_name="concept_id")
            .mutate(codeset_id=ibis.null().cast("int64"))
            .select("codeset_id", "concept_id")
        ),
    )
    try:
        # Create two trivial inclusion rules whose masks are always true.
        rules = [
            InclusionRule(name="r1", expression=CriteriaGroup()),
            InclusionRule(name="r2", expression=CriteriaGroup()),
        ]
        out = apply_inclusion_rules(con.table("events"), rules, ctx)
        sql = out.to_sql(dialect="postgres").upper()
        # Ensure the SUM() feeding the mask is explicitly cast back to BIGINT so `&` works on Postgres.
        assert "&" in sql
        assert "CAST(CAST(SUM" in sql
    finally:
        ctx.close()
