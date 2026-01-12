from __future__ import annotations

from mitos.sql_split import split_sql_statements


def test_split_sql_statements_handles_apostrophes_in_line_comments():
    sql = """\
CREATE TABLE t AS SELECT 1;
-- event's op end date; this semicolon should not split
DROP TABLE IF EXISTS t;
"""
    parts = split_sql_statements(sql)
    assert parts == [
        "CREATE TABLE t AS SELECT 1",
        "-- event's op end date; this semicolon should not split\nDROP TABLE IF EXISTS t",
    ]


def test_split_sql_statements_handles_block_comments():
    sql = """\
/* comment with ; and 'quotes' */
CREATE TABLE t AS SELECT 1;
DROP TABLE IF EXISTS t;
"""
    parts = split_sql_statements(sql)
    assert parts == [
        "/* comment with ; and 'quotes' */\nCREATE TABLE t AS SELECT 1",
        "DROP TABLE IF EXISTS t",
    ]
