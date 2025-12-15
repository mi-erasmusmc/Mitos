from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Set

import polars as pl

from mitos.cohort_expression import ConceptSet


@dataclass(frozen=True)
class MinimalVocab:
    concept: pl.DataFrame
    concept_ancestor: pl.DataFrame
    concept_relationship: pl.DataFrame


def _collect_concept_ids(concept_sets: Iterable[ConceptSet]) -> Set[int]:
    ids: set[int] = set()
    for cs in concept_sets or []:
        expr = getattr(cs, "expression", None)
        items = getattr(expr, "items", None) or []
        for item in items:
            concept = getattr(item, "concept", None)
            concept_id = getattr(concept, "concept_id", None)
            if concept_id is None:
                continue
            ids.add(int(concept_id))
    return ids


def build_minimal_vocab(concept_sets: list[ConceptSet]) -> MinimalVocab:
    """
    Build a tiny but functional OMOP vocabulary for tests:
    - `concept` contains all concept ids in the phenotype JSON and marks them valid.
    - `concept_ancestor` contains identity relationships only, so includeDescendants includes itself.
    - `concept_relationship` contains identity 'Maps to' rows only, so includeMapped includes itself.
    """
    concept_ids = sorted(_collect_concept_ids(concept_sets))

    concept = pl.DataFrame(
        {
            "concept_id": concept_ids,
            "invalid_reason": [None] * len(concept_ids),
        },
        schema={
            "concept_id": pl.Int64,
            "invalid_reason": pl.Utf8,
        },
    )
    concept_ancestor = pl.DataFrame(
        {
            "ancestor_concept_id": concept_ids,
            "descendant_concept_id": concept_ids,
        },
        schema={
            "ancestor_concept_id": pl.Int64,
            "descendant_concept_id": pl.Int64,
        },
    )
    concept_relationship = pl.DataFrame(
        {
            "concept_id_1": concept_ids,
            "concept_id_2": concept_ids,
            "relationship_id": ["Maps to"] * len(concept_ids),
            "invalid_reason": [None] * len(concept_ids),
        },
        schema={
            "concept_id_1": pl.Int64,
            "concept_id_2": pl.Int64,
            "relationship_id": pl.Utf8,
            "invalid_reason": pl.Utf8,
        },
    )
    return MinimalVocab(
        concept=concept,
        concept_ancestor=concept_ancestor,
        concept_relationship=concept_relationship,
    )
