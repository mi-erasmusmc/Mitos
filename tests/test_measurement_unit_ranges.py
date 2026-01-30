from __future__ import annotations


import ibis

from mitos.build_context import BuildContext, CohortBuildOptions, CodesetResource
from mitos.builders.measurement import build_measurement
from mitos.ibis_compat import table_from_literal_list
from mitos.tables import Measurement


def test_measurement_value_range_respects_unit_specific_thresholds_without_normalization():
    """
    Regression: Circe uses unit-specific numeric ranges and does not normalize units.

    Phenotype-216 has inclusion rules with separate Measurement criteria for:
      - 10^9/L-style units with small numeric ranges (e.g. 0.01..1.499)
      - cells/uL-style units with large numeric ranges (e.g. 10..1500)

    If we normalize units, the second form gets scaled down and stops matching.
    """
    con = ibis.duckdb.connect(database=":memory:")

    # Minimal measurement table.
    con.raw_sql(
        """
        CREATE TABLE measurement (
          measurement_id BIGINT,
          person_id BIGINT,
          measurement_concept_id BIGINT,
          measurement_date DATE,
          unit_concept_id BIGINT,
          value_as_number DOUBLE,
          visit_occurrence_id BIGINT
        )
        """
    )
    con.raw_sql(
        """
        INSERT INTO measurement VALUES
          (1, 1, 123, DATE '2020-01-01', 8784, 500.0, 0)
        """
    )

    # Empty codesets (we don't filter by codeset_id in this test).
    empty_concepts = table_from_literal_list([], column_name="concept_id", element_type="int64")
    empty_codesets = empty_concepts.mutate(codeset_id=ibis.null().cast("int64")).select(
        "codeset_id", "concept_id"
    )
    codesets = CodesetResource(table=empty_codesets)
    options = CohortBuildOptions(cdm_schema="main", vocabulary_schema="main", backend="duckdb")
    ctx = BuildContext(con, options, codesets)
    try:
        criteria = Measurement.model_validate(
            {
                "CodesetId": None,
                "ValueAsNumber": {"Value": 10, "Op": "bt", "Extent": 1500},
                "Unit": [{"CONCEPT_ID": 8784}],
            }
        )
        events = build_measurement(criteria, ctx)
        assert events is not None
        assert int(events.count().execute()) == 1
    finally:
        ctx.close()
