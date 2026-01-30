from __future__ import annotations

from scripts.compare_cohort_counts import _exec_raw


class _FakeCursor:
    def __init__(self) -> None:
        self.closed = False

    def fetchall(self):
        return []

    def close(self):
        self.closed = True


class _FakeDuckBackend:
    """Mimic ibis duckdb backend behavior: raw_sql returns underlying connection."""

    def __init__(self) -> None:
        self.closed = False
        self.con = self

    def raw_sql(self, sql: str):
        return self

    def fetchall(self):
        return []

    def close(self):
        # Represents closing the underlying connection (should not happen per statement)
        self.closed = True


class _FakeDbxBackend:
    """Mimic a backend where raw_sql returns a closeable cursor."""

    def __init__(self) -> None:
        self.cursor = _FakeCursor()

    def raw_sql(self, sql: str):
        return self.cursor


def test_exec_raw_does_not_close_duckdb_connection():
    con = _FakeDuckBackend()
    _exec_raw(con, "SELECT 1")
    assert con.closed is False


def test_exec_raw_closes_cursor_for_other_backends():
    con = _FakeDbxBackend()
    _exec_raw(con, "SELECT 1")
    assert con.cursor.closed is True
