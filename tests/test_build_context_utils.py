from __future__ import annotations

import uuid

import pytest

from mitos.build_context import (
    BuildContext,
    CohortBuildOptions,
    _materialize_codesets,
    _qualify,
    _table,
    _union_all,
    _union_distinct,
    CodesetResource,
)

import ibis


class FakeTable:
    def __init__(self, value: str):
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"FakeTable({self.value})"


class FakeExpr:
    """Only used as an opaque object; BuildContext uses conn.compile(expr)."""

    def __init__(self, sql: str = "SELECT 1"):
        self._sql = sql


class DummyBackend:
    """
    Minimal stand-in for an ibis backend that records calls.

    table_behavior controls whether table() succeeds:
      - "database": succeed when database is provided
      - any other value -> always raise
    """

    def __init__(
        self,
        table_behavior: str = "raise",
        *,
        raw_sql_side_effects: list | None = None,
        drop_raises: int | bool = 0,
    ):
        self.table_behavior = table_behavior
        self.raw_sql_side_effects = list(raw_sql_side_effects or [])
        self.drop_raises = drop_raises
        self.calls: list[tuple] = []

    def table(self, name, *, database=None):
        self.calls.append(("table", name, database))
        if self.table_behavior == "always":
            suffix = f"{database}.{name}" if database else name
            return FakeTable(f"db:{suffix}")
        if self.table_behavior == "database" and database is not None:
            return FakeTable(f"db:{database}.{name}")
        raise Exception("table resolution failed")

    def sql(self, query: str):
        self.calls.append(("sql", query))
        return FakeTable(query)

    def raw_sql(self, sql: str):
        self.calls.append(("raw_sql", sql))
        if self.raw_sql_side_effects:
            effect = self.raw_sql_side_effects.pop(0)
            if isinstance(effect, BaseException):
                raise effect
            if effect == "raise":
                raise Exception("raw_sql failure")
        return None

    def compile(self, expr):
        # mimic backend compilation for capture_sql
        sql = getattr(expr, "_sql", "SELECT 1")
        self.calls.append(("compile", sql))
        return sql

    def create_table(self, name, *, obj, database=None, temp=False, overwrite=False):
        self.calls.append(("create_table", name, obj, database, temp, overwrite))
        return None

    def drop_table(self, name, *, database=None, force=False):
        self.calls.append(("drop_table", name, database, force))
        if self.drop_raises:
            if isinstance(self.drop_raises, int) and self.drop_raises > 0:
                self.drop_raises -= 1
            else:
                self.drop_raises = 0
            raise Exception("drop failure")
        return None


class UnionRecorder:
    def __init__(self, name: str, calls: list | None = None):
        self.name = name
        self.calls = [] if calls is None else calls

    def union(self, other, distinct=False):
        self.calls.append((self.name, getattr(other, "name", None), distinct))
        return UnionRecorder(f"{self.name}+{getattr(other, 'name', '?')}", self.calls)

    def __repr__(self):
        return f"UnionRecorder({self.name})"


def test_qualify_handles_none_string_and_tuple():
    assert _qualify(None, "events") == "events"
    assert _qualify("public", "events") == "public.events"
    assert _qualify("catalog.schema", "events") == "catalog.schema.events"
    assert _qualify(("catalog", "schema"), "events") == "catalog.schema.events"


def test_qualify_handles_underscores_and_uuid_names():
    underscored = "schema_with_underscores"
    name = "table_with_underscores"
    assert _qualify(underscored, name) == f"{underscored}.{name}"
    uuid_name = uuid.uuid4().hex
    assert _qualify("cat.sch", uuid_name) == f"cat.sch.{uuid_name}"


def test_module_table_calls_conn_table_with_database():
    conn = DummyBackend(table_behavior="database")
    tbl = _table(conn, "cat.schema", "concept")
    assert isinstance(tbl, FakeTable)
    assert tbl.value == "db:cat.schema.concept"
    assert conn.calls == [("table", "concept", "cat.schema")]


def test_module_table_never_calls_sql():
    conn = DummyBackend(table_behavior="database")
    _table(conn, "db.schema", "x")
    assert [c[0] for c in conn.calls] == ["table"]


def test_buildcontext_table_falls_back_to_sql_on_failure():
    options = CohortBuildOptions(cdm_schema="cat.schema")
    conn = DummyBackend(table_behavior="raise")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    t = ctx.table("concept")
    assert isinstance(t, FakeTable)

    assert conn.calls[0] == ("table", "concept", "cat.schema")
    assert conn.calls[1][0] == "sql"
    assert conn.calls[1][1].startswith("SELECT * FROM ")
    assert "cat.schema.concept" in conn.calls[1][1]


def test_buildcontext_table_success_does_not_call_sql():
    options = CohortBuildOptions(cdm_schema="cdm")
    conn = DummyBackend(table_behavior="database")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    t = ctx.table("person")
    assert isinstance(t, FakeTable)
    assert [c[0] for c in conn.calls] == ["table"]


def test_vocabulary_table_prefers_vocab_schema():
    options = CohortBuildOptions(cdm_schema="cdm", vocabulary_schema="vocab")
    conn = DummyBackend(table_behavior="database")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    ctx.vocabulary_table("concept")
    assert ("table", "concept", "vocab") in conn.calls

    conn2 = DummyBackend(table_behavior="database")
    ctx2 = BuildContext(
        conn2, CohortBuildOptions(cdm_schema="cdm"), FakeTable("codesets")
    )
    ctx2.vocabulary_table("concept")
    assert ("table", "concept", "cdm") in conn2.calls


def test_codeset_resource_cleanup_idempotent_and_handles_exceptions():
    class Recorder:
        def __init__(self):
            self.calls = 0

        def __call__(self):
            self.calls += 1
            if self.calls == 1:
                raise Exception("boom")

    dropper = Recorder()
    resource = CodesetResource(table=FakeTable("t"), _dropper=dropper)

    with pytest.raises(Exception):
        resource.cleanup()
    resource.cleanup()
    assert dropper.calls == 1
    assert resource._dropper is None


def test_materialize_codesets_uses_create_table_with_database_and_drop_table():
    options = CohortBuildOptions(
        temp_emulation_schema="catalog.schema", backend="databricks"
    )
    conn = DummyBackend(table_behavior="database")
    expr = FakeExpr("SELECT 1")

    resource = _materialize_codesets(conn, expr, options)

    # create_table called with database="catalog.schema", temp=False, overwrite=True
    create_calls = [c for c in conn.calls if c[0] == "create_table"]
    assert len(create_calls) == 1
    _, name, obj, database, temp, overwrite = create_calls[0]
    assert name.startswith("_codesets_")
    assert obj is expr
    assert database == "catalog.schema"
    assert temp is False
    assert overwrite is True

    # table lookup should be by database
    assert ("table", name, "catalog.schema") in conn.calls

    # analyze call emitted for databricks
    assert (
        "raw_sql",
        f"ANALYZE TABLE catalog.schema.{name} COMPUTE STATISTICS",
    ) in conn.calls

    # cleanup should drop by database with force=True
    resource.cleanup()
    assert ("drop_table", name, "catalog.schema", True) in conn.calls


def test_materialize_codesets_uses_temp_tables_when_no_emulation():
    options = CohortBuildOptions(backend="duckdb")
    conn = DummyBackend(table_behavior="always")
    expr = FakeExpr("SELECT 1")

    resource = _materialize_codesets(conn, expr, options)

    create_calls = [c for c in conn.calls if c[0] == "create_table"]
    assert len(create_calls) == 1
    _, name, obj, database, temp, overwrite = create_calls[0]
    assert obj is expr
    assert database is None
    assert temp is True
    assert overwrite is True
    assert ("table", name, None) in conn.calls

    # analyze call emitted for duckdb
    assert ("raw_sql", f"ANALYZE {name}") in conn.calls

    resource.cleanup()
    assert ("drop_table", name, None, True) in conn.calls


def test_materialize_respects_temp_emulation_schema_for_stages():
    options = CohortBuildOptions(
        temp_emulation_schema="catalog.schema",
        backend="duckdb",
        capture_sql=True,
    )
    conn = DummyBackend(table_behavior="database")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    table = ctx.materialize(FakeExpr("SELECT 1"), label="t1", temp=True, analyze=True)
    assert isinstance(table, FakeTable)

    create_calls = [c for c in conn.calls if c[0] == "create_table"]
    assert len(create_calls) == 1
    _, table_name, obj, database, temp, overwrite = create_calls[0]
    assert table_name.startswith("_stage_t1_")
    assert database == "catalog.schema"
    # temp emulation => temp=False
    assert temp is False
    assert overwrite is True

    # capture_sql => compile called
    assert ("compile", "SELECT 1") in conn.calls

    # analyze emitted for duckdb
    analyze_calls = [
        c for c in conn.calls if c[0] == "raw_sql" and c[1].startswith("ANALYZE ")
    ]
    assert analyze_calls
    assert f"ANALYZE catalog.schema.{table_name}" == analyze_calls[0][1]

    ctx.close()
    # drop_table called during close (force=True, with database)
    assert ("drop_table", table_name, "catalog.schema", True) in conn.calls


@pytest.mark.parametrize(
    "temp_emulation_schema,temp_flag,expected_db,expected_temp",
    [
        (None, True, None, True),
        ("cat.schema", True, "cat.schema", False),
        ("cat.schema", False, None, False),
    ],
)
def test_materialize_core_temp_combinations(
    temp_emulation_schema, temp_flag, expected_db, expected_temp
):
    options = CohortBuildOptions(temp_emulation_schema=temp_emulation_schema)
    conn = DummyBackend(table_behavior="always")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    ctx.materialize(FakeExpr("SELECT 1"), label="core", temp=temp_flag, analyze=False)

    create_calls = [c for c in conn.calls if c[0] == "create_table"]
    assert len(create_calls) == 1
    _, _, _, database, temp, overwrite = create_calls[0]
    assert database == expected_db
    assert temp is expected_temp
    assert overwrite is True


def test_capture_sql_branch_only_compiles_when_enabled():
    options = CohortBuildOptions(capture_sql=True)
    conn = DummyBackend(table_behavior="always")
    ctx = BuildContext(conn, options, FakeTable("codesets"))
    ctx.materialize(FakeExpr("SELECT 42"), label="cap", temp=False, analyze=False)

    assert ("compile", "SELECT 42") in conn.calls
    captured = ctx.captured_sql()
    assert len(captured) == 1
    table_name, sql = captured[0]
    assert table_name.startswith("_stage_cap_")
    assert sql == "SELECT 42"

    options_off = CohortBuildOptions(capture_sql=False)
    conn2 = DummyBackend(table_behavior="always")
    ctx2 = BuildContext(conn2, options_off, FakeTable("codesets"))
    ctx2.materialize(FakeExpr("SELECT 0"), label="nocap", temp=False, analyze=False)
    assert not any(c[0] == "compile" for c in conn2.calls)
    assert ctx2.captured_sql() == []


def test_profiling_branch_runs_for_duckdb_and_skips_for_others(tmp_path):
    options = CohortBuildOptions(backend="duckdb", profile_dir=str(tmp_path))
    conn = DummyBackend(table_behavior="always")
    ctx = BuildContext(conn, options, FakeTable("codesets"))
    ctx.materialize(FakeExpr("SELECT 1"), label="prof", temp=False, analyze=False)

    raw_calls = [c[1] for c in conn.calls if c[0] == "raw_sql"]
    assert any("SET profiling_output" in call for call in raw_calls)
    assert any("SET enable_profiling" in call for call in raw_calls)
    assert any("SET profiling_coverage" in call for call in raw_calls)
    assert any("disable_profiling" in call for call in raw_calls)

    options_pg = CohortBuildOptions(backend="postgres", profile_dir=str(tmp_path))
    conn_pg = DummyBackend(table_behavior="always")
    ctx_pg = BuildContext(conn_pg, options_pg, FakeTable("codesets"))
    ctx_pg.materialize(FakeExpr("SELECT 1"), label="noprof", temp=False, analyze=False)
    assert not any("profiling" in c[1] for c in conn_pg.calls if c[0] == "raw_sql")


def test_profiling_enable_failure_does_not_block_materialize(tmp_path):
    conn = DummyBackend(
        table_behavior="always", raw_sql_side_effects=[Exception("fail profiling")]
    )
    options = CohortBuildOptions(backend="duckdb", profile_dir=str(tmp_path))
    ctx = BuildContext(conn, options, FakeTable("codesets"))
    ctx.materialize(FakeExpr("SELECT 1"), label="prof", temp=False, analyze=False)

    assert any(c[0] == "create_table" for c in conn.calls)


def test_analyze_branch_per_backend_and_failure_handling():
    # duckdb/postgres variant
    for backend in ("duckdb", "postgres"):
        conn = DummyBackend(table_behavior="always")
        options = CohortBuildOptions(backend=backend)
        ctx = BuildContext(conn, options, FakeTable("codesets"))
        ctx.materialize(FakeExpr("SELECT 1"), label="an", temp=False, analyze=True)
        raw_calls = [c[1] for c in conn.calls if c[0] == "raw_sql"]
        assert any(call.startswith("ANALYZE ") for call in raw_calls)

    # databricks variant
    conn_db = DummyBackend(table_behavior="always")
    options_db = CohortBuildOptions(backend="databricks")
    ctx_db = BuildContext(conn_db, options_db, FakeTable("codesets"))
    ctx_db.materialize(FakeExpr("SELECT 1"), label="an", temp=False, analyze=True)
    raw_calls_db = [c[1] for c in conn_db.calls if c[0] == "raw_sql"]
    assert any(
        call.startswith("ANALYZE TABLE ") and "COMPUTE STATISTICS" in call
        for call in raw_calls_db
    )

    # analyze failure swallowed
    conn_fail = DummyBackend(
        table_behavior="always", raw_sql_side_effects=[Exception("analyze fail")]
    )
    options_fail = CohortBuildOptions(backend="postgres")
    ctx_fail = BuildContext(conn_fail, options_fail, FakeTable("codesets"))
    ctx_fail.materialize(FakeExpr("SELECT 1"), label="an", temp=False, analyze=True)
    assert any(c[0] == "create_table" for c in conn_fail.calls)


def test_materialize_registers_cleanup_and_close_cleans_up():
    drop_backend = DummyBackend(table_behavior="always", drop_raises=1)
    options = CohortBuildOptions()
    dropper_called = []
    codesets = CodesetResource(
        table=FakeTable("codesets"), _dropper=lambda: dropper_called.append("codeset")
    )
    ctx = BuildContext(drop_backend, options, codesets)

    table = ctx.materialize(FakeExpr("SELECT 1"), label="cl", temp=False, analyze=False)
    assert isinstance(table, FakeTable)
    assert len(ctx._cleanup_callbacks) == 1
    ctx.close()
    assert any(c[0] == "drop_table" for c in drop_backend.calls)
    assert dropper_called == ["codeset"]
    assert ctx._cleanup_callbacks == []
    assert ctx._captured_sql == []
    assert ctx._slice_cache == {}


def test_materialize_codesets_temp_flags_match_emulation_setting():
    options = CohortBuildOptions(temp_emulation_schema="cat.schema")
    conn = DummyBackend(table_behavior="database")
    expr = FakeExpr("SELECT 1")
    resource = _materialize_codesets(conn, expr, options)
    create = [c for c in conn.calls if c[0] == "create_table"][0]
    _, name, _, database, temp, overwrite = create
    assert database == "cat.schema"
    assert temp is False
    assert overwrite is True
    resource.cleanup()
    assert ("drop_table", name, "cat.schema", True) in conn.calls


def test_union_helpers_handle_none_and_distinct_flags():
    assert _union_distinct([]) is None
    assert _union_distinct([None, None]) is None

    t1 = UnionRecorder("t1")
    t2 = UnionRecorder("t2")
    t3 = UnionRecorder("t3")
    combined = _union_distinct([t1, None, t2, t3])
    assert isinstance(combined, UnionRecorder)
    assert any(call[2] is True for call in combined.calls)

    a = UnionRecorder("a")
    b = UnionRecorder("b")
    c = UnionRecorder("c")
    combined_all = _union_all([a, b, c])
    assert isinstance(combined_all, UnionRecorder)
    assert any(call[2] is False for call in combined_all.calls)


def test_write_cohort_table_creates_result_table_in_schema():
    events = ibis.memtable(
        [{"person_id": 1, "start_date": "2020-01-01", "end_date": "2020-01-02"}],
        schema={
            "person_id": "int64",
            "start_date": "timestamp",
            "end_date": "timestamp",
        },
    )
    options = CohortBuildOptions(
        result_schema="scratch.schema",
        target_table="cohort_table",
        cohort_id=123,
    )
    conn = DummyBackend(table_behavior="database")
    ctx = BuildContext(conn, options, FakeTable("codesets"))

    tbl = ctx.write_cohort_table(events)
    assert isinstance(tbl, FakeTable)

    create_calls = [c for c in conn.calls if c[0] == "create_table"]
    assert len(create_calls) == 1
    _, name, obj, database, temp, overwrite = create_calls[0]
    assert name == "cohort_table"
    assert database == "scratch.schema"
    assert temp is False
    assert overwrite is True
