from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Callable, Iterable, Optional
import uuid
import weakref

import ibis
import ibis.expr.types as ir

from .concept_set import ConceptSet


@dataclass(frozen=True)
class CohortBuildOptions:
    cdm_schema: Optional[str] = None
    vocabulary_schema: Optional[str] = None
    result_schema: Optional[str] = None
    target_table: Optional[str] = None
    cohort_id: Optional[int] = None
    generate_stats: bool = False
    temp_emulation_schema: Optional[str] = None


@dataclass
class CodesetResource:
    table: ir.Table
    _dropper: Optional[Callable[[], None]] = None

    def cleanup(self):
        if self._dropper:
            try:
                self._dropper()
            finally:
                self._dropper = None


class BuildContext:
    """Holds shared state (connection, schemas, compiled codesets) used across builders."""

    def __init__(self, conn: ibis.BaseBackend, options: CohortBuildOptions, codeset_resource: CodesetResource | ir.Table):
        self._conn = conn
        self._options = options
        if isinstance(codeset_resource, CodesetResource):
            self._codeset_resource = codeset_resource
        else:
            self._codeset_resource = CodesetResource(table=codeset_resource)
        self._codesets = self._codeset_resource.table
        self._cleanup_callbacks: list[Callable[[], None]] = []
        weakref.finalize(self, self.close)

    def _table(self, schema: Optional[str], name: str) -> ir.Table:
        if schema:
            try:
                return self._conn.table(name, schema=schema)
            except TypeError:
                qualified = f"{_quote_identifier(schema)}.{_quote_identifier(name)}"
                return self._conn.sql(f"SELECT * FROM {qualified}")
        return self._conn.table(name)

    def table(self, name: str) -> ir.Table:
        """Return a CDM table."""
        return self._table(self._options.cdm_schema, name)

    def vocabulary_table(self, name: str) -> ir.Table:
        """Return a vocabulary table (concept, concept_ancestor, etc.)."""
        schema = self._options.vocabulary_schema or self._options.cdm_schema
        return self._table(schema, name)

    def codeset(self, codeset_id: int, *, is_exclusion: bool = False) -> ir.Table:
        """Return concepts for the requested codeset. `is_exclusion` is provided for parity with Circe."""
        _ = is_exclusion  # placeholder for future differentiated handling
        return self._codesets.filter(self._codesets.codeset_id == codeset_id)

    @property
    def codesets(self) -> ir.Table:
        return self._codesets

    @property
    def conn(self) -> ibis.BaseBackend:
        return self._conn

    def options(self) -> CohortBuildOptions:
        return self._options

    def register_cleanup(self, callback: Callable[[], None]):
        self._cleanup_callbacks.append(callback)

    def close(self):
        if self._codeset_resource is not None:
            self._codeset_resource.cleanup()
            self._codeset_resource = None  # type: ignore[assignment]
        while self._cleanup_callbacks:
            callback = self._cleanup_callbacks.pop()
            try:
                callback()
            except Exception:
                pass


def compile_codesets(
    conn: ibis.BaseBackend,
    concept_sets: list[ConceptSet],
    options: CohortBuildOptions,
) -> CodesetResource:
    """Rebuild Circe concept set logic as an ibis expression."""

    vocab_schema = options.vocabulary_schema or options.cdm_schema
    concept = _table(conn, vocab_schema, "concept")
    concept_ancestor = _table(conn, vocab_schema, "concept_ancestor")
    concept_relationship = _table(conn, vocab_schema, "concept_relationship")

    compiled = []
    for concept_set in concept_sets or []:
        compiled_expr = _compile_single_codeset(
            concept, concept_ancestor, concept_relationship, concept_set
        )
        if compiled_expr is not None:
            compiled.append(compiled_expr)

    if not compiled:
        compiled_expr = _empty_codeset_table()
    else:
        compiled_expr = _union_all(compiled).distinct()

    return _materialize_codesets(conn, compiled_expr, options)


def _compile_single_codeset(
    concept: ir.Table,
    concept_ancestor: ir.Table,
    concept_relationship: ir.Table,
    concept_set: ConceptSet,
) -> Optional[ir.Table]:
    expression = concept_set.expression
    if expression is None or not expression.items:
        return None

    include_ids: list[int] = []
    include_descendant_ids: list[int] = []
    include_mapped_ids: list[int] = []
    include_mapped_descendant_ids: list[int] = []

    exclude_ids: list[int] = []
    exclude_descendant_ids: list[int] = []
    exclude_mapped_ids: list[int] = []
    exclude_mapped_descendant_ids: list[int] = []

    for item in expression.items:
        if item.concept is None or item.concept.concept_id is None:
            continue
        target_include = not bool(item.is_excluded)
        include_descendants = bool(item.include_descendants)
        include_mapped = bool(item.include_mapped)
        concept_id = int(item.concept.concept_id)

        if target_include:
            include_ids.append(concept_id)
            if include_descendants:
                include_descendant_ids.append(concept_id)
            if include_mapped:
                include_mapped_ids.append(concept_id)
                if include_descendants:
                    include_mapped_descendant_ids.append(concept_id)
        else:
            exclude_ids.append(concept_id)
            if include_descendants:
                exclude_descendant_ids.append(concept_id)
            if include_mapped:
                exclude_mapped_ids.append(concept_id)
                if include_descendants:
                    exclude_mapped_descendant_ids.append(concept_id)

    include_expr = _union_distinct(
        [
            _ids_memtable(include_ids),
            _descendants(concept, concept_ancestor, include_descendant_ids),
            _mapped_concepts(
                concept,
                concept_ancestor,
                concept_relationship,
                include_mapped_ids,
                include_mapped_descendant_ids,
            ),
        ]
    )

    if include_expr is None:
        return None

    exclude_expr = _union_distinct(
        [
            _ids_memtable(exclude_ids),
            _descendants(concept, concept_ancestor, exclude_descendant_ids),
            _mapped_concepts(
                concept,
                concept_ancestor,
                concept_relationship,
                exclude_mapped_ids,
                exclude_mapped_descendant_ids,
            ),
        ]
    )

    if exclude_expr is not None:
        include_expr = include_expr.anti_join(exclude_expr, ["concept_id"])

    codeset_literal = ibis.literal(int(concept_set.id), type="int64")
    return include_expr.mutate(codeset_id=codeset_literal)[["codeset_id", "concept_id"]]


def _table(conn: ibis.BaseBackend, schema: Optional[str], name: str) -> ir.Table:
    if schema:
        try:
            return conn.table(name, schema=schema)
        except TypeError:
            qualified = f"{_quote_identifier(schema)}.{_quote_identifier(name)}"
            return conn.sql(f"SELECT * FROM {qualified}")
    return conn.table(name)


def _ids_memtable(ids: list[int]) -> Optional[ir.Table]:
    if not ids:
        return None
    schema = ibis.schema({"concept_id": "int64"})
    return ibis.memtable([{"concept_id": i} for i in ids], schema=schema).distinct()


def _descendants(
    concept: ir.Table, concept_ancestor: ir.Table, ancestor_ids: list[int]
) -> Optional[ir.Table]:
    if not ancestor_ids:
        return None
    return (
        concept_ancestor.filter(concept_ancestor.ancestor_concept_id.isin(ancestor_ids))
        .join(concept, concept_ancestor.descendant_concept_id == concept.concept_id)
        .filter(concept.invalid_reason.isnull())
        .select(concept.concept_id.cast("int64").name("concept_id"))
        .distinct()
    )


def _mapped_concepts(
    concept: ir.Table,
    concept_ancestor: ir.Table,
    concept_relationship: ir.Table,
    concepts_to_map: list[int],
    concepts_with_descendants_to_map: list[int],
) -> Optional[ir.Table]:
    sources = _union_distinct(
        [
            _ids_memtable(concepts_to_map),
            _descendants(concept, concept_ancestor, concepts_with_descendants_to_map),
        ]
    )

    if sources is None:
        return None

    valid_relationships = concept_relationship.filter(
        [
            concept_relationship.relationship_id == "Maps to",
            concept_relationship.invalid_reason.isnull(),
        ]
    )

    return (
        sources.join(valid_relationships, sources.concept_id == valid_relationships.concept_id_2)
        .select(valid_relationships.concept_id_1.cast("int64").name("concept_id"))
        .distinct()
    )


def _empty_codeset_table() -> ir.Table:
    schema = ibis.schema({"codeset_id": "int64", "concept_id": "int64"})
    return ibis.memtable([], schema=schema)


def _materialize_codesets(
    conn: ibis.BaseBackend,
    expr: ir.Table,
    options: CohortBuildOptions,
) -> CodesetResource:
    name = f"_codesets_{uuid.uuid4().hex}"
    if options.temp_emulation_schema:
        schema = options.temp_emulation_schema
        qualified = f'"{schema}"."{name}"'
        conn.raw_sql(f"CREATE TABLE {qualified} AS {expr.compile()}")
        table = conn.table(name, schema=schema)
        drop_sql = f"DROP TABLE IF EXISTS {qualified}"
    else:
        conn.create_table(name, expr, temp=True, overwrite=True)
        table = conn.table(name)
        drop_sql = f'DROP TABLE IF EXISTS "{name}"'

    def _drop():
        try:
            conn.raw_sql(drop_sql)
        except Exception:
            pass

    resource = CodesetResource(table=table, _dropper=_drop)
    weakref.finalize(resource.table, resource.cleanup)
    return resource


def _quote_identifier(identifier: str) -> str:
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _union_distinct(tables: Iterable[Optional[ir.Table]]) -> Optional[ir.Table]:
    valid_tables = [t for t in tables if t is not None]
    if not valid_tables:
        return None

    return reduce(lambda left, right: left.union(right), valid_tables[1:], valid_tables[0]).distinct()


def _union_all(tables: list[ir.Table]) -> ir.Table:
    return reduce(lambda left, right: left.union(right), tables[1:], tables[0])
