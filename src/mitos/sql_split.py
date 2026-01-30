from __future__ import annotations


def split_sql_statements(sql_script: str) -> list[str]:
    """
    Split a SQL script into individual statements, handling:
      - `-- ...` line comments
      - `/* ... */` block comments
      - string literals ('...', "...") and backticks (`...`)

    This is intentionally conservative: it only treats `;` as a statement separator
    when not inside a string/comment context.
    """
    statements: list[str] = []
    current: list[str] = []
    in_single = in_double = in_backtick = False
    in_line_comment = in_block_comment = False
    escape = False
    i = 0
    n = len(sql_script)
    while i < n:
        ch = sql_script[i]
        nxt = sql_script[i + 1] if i + 1 < n else ""
        current.append(ch)
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                current.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if not (in_single or in_double or in_backtick):
            if ch == "-" and nxt == "-":
                current.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                current.append(nxt)
                in_block_comment = True
                i += 2
                continue

        if ch == "\\" and not escape:
            escape = True
            i += 1
            continue

        if ch == "'" and not (in_double or in_backtick) and not escape:
            in_single = not in_single
        elif ch == '"' and not (in_single or in_backtick) and not escape:
            in_double = not in_double
        elif ch == "`" and not (in_single or in_double) and not escape:
            in_backtick = not in_backtick
        elif ch == ";" and not (in_single or in_double or in_backtick):
            if s := "".join(current).strip():
                statements.append(s[:-1].strip())
            current = []

        escape = False
        i += 1
    if s := "".join(current).strip():
        statements.append(s)
    return statements
