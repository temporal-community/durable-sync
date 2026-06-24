"""Neutral `Schema` -> Notion `CREATE TABLE (...)` DDL — the Notion half of Layer 2.

Pure + unit-testable (no I/O): `NotionDestination.ensure_schema` calls this to turn
a neutral schema into the `schema` argument the `notion-create-database` MCP tool
takes (the exact shape hand-written in tests/smoke_notion.py). Kept separate from
destination.py so the mapping can be tested without a live MCP session.
"""
from __future__ import annotations

from durable_sync.schema import Kind, Role, Schema

# Neutral kind -> Notion DDL column type. Role.TITLE overrides this (Notion requires
# exactly one TITLE column, whatever the underlying value kind).
_DDL_TYPE: dict[Kind, str] = {
    Kind.TEXT: "RICH_TEXT",
    Kind.NUMBER: "NUMBER",
    Kind.CHECKBOX: "CHECKBOX",
    Kind.MULTI_SELECT: "MULTI_SELECT",
    Kind.SELECT: "SELECT",
    Kind.DATE: "DATE",
}


def _quote(name: str) -> str:
    """Double-quote a column name, doubling any embedded quote (SQL convention)."""
    return '"' + name.replace('"', '""') + '"'


def column_ddl_type(column) -> str:
    """The Notion DDL type for one neutral Column (TITLE wins over kind)."""
    if column.role is Role.TITLE:
        return "TITLE"
    return _DDL_TYPE[column.kind]


def schema_to_ddl(schema: Schema) -> str:
    """Render a neutral Schema as `CREATE TABLE ("Col" TYPE, ...)`.

    Notion needs exactly one TITLE column; a schema from `infer_schema` always has
    one (title is always emitted), so we don't synthesize one here — but we do
    guard against zero, which would produce a database the API rejects.
    """
    if not any(c.role is Role.TITLE for c in schema):
        raise ValueError("Notion schema needs exactly one TITLE column")
    cols = ", ".join(f"{_quote(c.name)} {column_ddl_type(c)}" for c in schema)
    return f"CREATE TABLE ({cols})"
