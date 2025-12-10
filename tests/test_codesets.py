import ibis

from ibis_cohort.concept_set import ConceptSet, ConceptSetExpression, ConceptSetItem
from ibis_cohort.criteria import Concept
from ibis_cohort.build_context import CohortBuildOptions, compile_codesets


def test_compile_codesets_handles_descendants_and_exclusions():
    conn = ibis.duckdb.connect(database=":memory:")

    concept_expr = ibis.memtable(
        {
            "concept_id": [1, 2, 3, 4],
            "invalid_reason": [None, None, None, None],
        },
        schema=ibis.schema(
            {
                "concept_id": "int64",
                "invalid_reason": "string",
            }
        ),
    )
    concept_ancestor_expr = ibis.memtable(
        {"ancestor_concept_id": [1], "descendant_concept_id": [3]},
        schema=ibis.schema(
            {
                "ancestor_concept_id": "int64",
                "descendant_concept_id": "int64",
            }
        ),
    )
    concept_relationship_expr = ibis.memtable(
        {
            "concept_id_1": [4],
            "concept_id_2": [1],
            "relationship_id": ["Maps to"],
            "invalid_reason": [None],
        },
        schema=ibis.schema(
            {
                "concept_id_1": "int64",
                "concept_id_2": "int64",
                "relationship_id": "string",
                "invalid_reason": "string",
            }
        ),
    )

    conn.create_table("concept", concept_expr, overwrite=True)
    conn.create_table("concept_ancestor", concept_ancestor_expr, overwrite=True)
    conn.create_table("concept_relationship", concept_relationship_expr, overwrite=True)

    expression = ConceptSetExpression(
        items=[
            ConceptSetItem.model_validate(
                {
                    "concept": Concept.model_validate({"concept_id": 1}),
                    "include_descendants": True,
                    "include_mapped": True,
                }
            ),
            ConceptSetItem.model_validate(
                {
                    "concept": Concept.model_validate({"concept_id": 3}),
                    "is_excluded": True,
                }
            ),
        ]
    )
    concept_set = ConceptSet(id=1, name="test", expression=expression)

    options = CohortBuildOptions()
    resource = compile_codesets(conn, [concept_set], options)
    table = resource.table
    result = (
        table.filter(table["codeset_id"] == ibis.literal(1))
        .order_by(table["concept_id"])
        .to_polars()
    )

    assert result["concept_id"].to_list() == [1, 4]
