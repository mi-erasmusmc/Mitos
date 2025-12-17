from __future__ import annotations

import ibis

from mitos.build_context import BuildContext, CohortBuildOptions, compile_codesets
from mitos.builders.pipeline import build_primary_events

from tests.scenarios.phenotype_218 import build_fake_omop_for_phenotype_218


def test_phenotype_218_synthetic_dataset_expected_people():
    con = ibis.duckdb.connect(database=":memory:")

    expression, expectations = build_fake_omop_for_phenotype_218(con, schema="main")

    options = CohortBuildOptions(
        cdm_schema="main",
        vocabulary_schema="main",
        backend="duckdb",
        materialize_stages=False,
        materialize_codesets=True,
    )
    codesets = compile_codesets(con, expression.concept_sets, options)
    ctx = BuildContext(con, options, codesets)
    try:
        events = build_primary_events(expression, ctx)
        assert events is not None

        df = events.execute()
        got_people = set(int(v) for v in df["person_id"].tolist())

        assert expectations.include_person_ids.issubset(got_people)
        assert got_people.isdisjoint(expectations.exclude_person_ids)
    finally:
        ctx.close()

