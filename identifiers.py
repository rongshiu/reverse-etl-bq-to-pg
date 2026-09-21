"""Render Python values as SQL identifiers and literals.

Shared by the DuckDB load path and the swap machinery, both of which build
statements as text: the swap renames catalog objects, and DuckDB's
`postgres_execute` takes a statement as a string literal.
"""

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def split_table(qualified: str) -> tuple[str, str]:
    """Split `schema.table` (or a bare `table`) into unquoted schema and table parts."""
    parts = [part.strip().strip('"') for part in qualified.split(".")]
    if len(parts) == 1:
        return "public", parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Cannot parse table name {qualified!r}; expected `table` or `schema.table`.")


def sql_literal(value: str) -> str:
    """Render a Python string as a single-quoted SQL literal."""
    return "'" + value.replace("'", "''") + "'"
