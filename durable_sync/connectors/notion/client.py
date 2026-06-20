"""Notion-specific MCP helpers — the bits the generic transport can't own:
Notion's SQL query shape, its collection:// data sources + row parsing, the
database->data-source resolution, and the value decode that inverts the
destination's encoding. The generic MCP session/transport now lives in
durable_sync.transport.mcp (Contentful rides the same transport).

Used by BOTH the destination (write) and source (read), sharing one MCP session.
"""
from __future__ import annotations

import json
import re
from typing import Any

from durable_sync.transport.mcp import McpSession, TokenProvider, open_session as _open_session
from durable_sync.connectors.notion import oauth

# Back-compat alias: NotionMCP is just the generic session.
NotionMCP = McpSession


def open_session(token_provider: TokenProvider):
    """Open a Notion-MCP session (the generic transport, pinned to Notion's endpoint)."""
    return _open_session(oauth.MCP_ENDPOINT, token_provider)


def query_sql(data_source_id: str, *, order_by: str | None = None, limit: int = 100, offset: int = 0) -> str:
    """A SELECT over one data source. ORDER BY a stable column is REQUIRED when
    paginating — unordered OFFSET reshuffles under concurrent edits and skips rows."""
    order = f' ORDER BY "{order_by}"' if order_by else ""
    return (f'SELECT * FROM "collection://{data_source_id}"'
            f'{order} LIMIT {limit} OFFSET {offset}')


# --- Result parsing (query results come back as JSON or, defensively, markdown) ---

def rows_from_result(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return rows_from_markdown(raw)
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        for key in ("results", "rows", "data"):
            if isinstance(data.get(key), list):
                return [d for d in data[key] if isinstance(d, dict)]
    return []


def rows_from_markdown(raw: str) -> list[dict[str, Any]]:
    lines = [ln for ln in raw.splitlines() if ln.strip().startswith("|")]
    if len(lines) < 2:
        return []
    headers = [h.strip() for h in lines[0].strip("|").split("|")]
    rows: list[dict[str, Any]] = []
    for ln in lines[2:]:
        cells = [c.strip() for c in ln.strip("|").split("|")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))
    return rows


# Columns Notion's query uses to carry a row's page id / url.
_ID_KEYS = ("id", "page_id", "_id", "url", "page_url")


def page_id_from_row(row: dict[str, Any]) -> str | None:
    for key in _ID_KEYS:
        val = row.get(key)
        if isinstance(val, str) and val:
            return val.rsplit("/", 1)[-1].split("?")[0]
    return None


def row_columns(row: dict[str, Any]) -> dict[str, Any]:
    """A row's user-facing columns, with the id/url plumbing keys removed."""
    return {k: v for k, v in row.items() if k not in _ID_KEYS}


# --- database -> data source resolution -------------------------------------
# A database id (what's in a Notion URL) is NOT a data source id (what the query
# tools want, as collection://<id>). Under the 2025-09-03 API they differ, and
# `collection://<database-id>` fails with "Data source not found".

_COLLECTION_RE = re.compile(r"collection://([0-9a-f-]{32,36})")


async def resolve_data_source_id(session: NotionMCP, id_or_url: str) -> str:
    """Resolve a database id/URL (or a data source id / collection URL) to the data
    source id the query & create tools need. Best-effort and SAFE: a collection://
    URL is just stripped; otherwise we `notion-fetch` the id and take the first
    `collection://<uuid>` from the result. On any failure — fetch unavailable, no
    match — we return the input UNCHANGED, so a correct id still works and
    resolution never makes things worse. A multi-source database resolves to its
    FIRST data source; pass a specific data source id to target another."""
    if id_or_url.startswith("collection://"):
        return id_or_url[len("collection://"):]
    try:
        raw = await session.call("notion-fetch", {"id": id_or_url})
    except Exception:
        return id_or_url
    m = _COLLECTION_RE.search(raw)
    return m.group(1) if m else id_or_url


# --- value decode (inverse of the destination's _encode, best-effort) -------

def decode_value(value: Any) -> Any:
    """Turn the strings the MCP query renders back into neutral types:
    `__YES__`/`__NO__` -> bool, a JSON array-of-strings -> list[str] (multi-select).
    Numbers/dates stay as the query's text — without the column's schema we can't
    tell a number column from a text one, so we don't guess and risk corrupting
    real text. Mirrors NotionDestination._encode for the cases it can recover."""
    if value == "__YES__":
        return True
    if value == "__NO__":
        return False
    if isinstance(value, str) and value[:1] == "[":
        try:
            parsed = json.loads(value)
        except ValueError:
            return value
        if isinstance(parsed, list) and all(isinstance(x, str) for x in parsed):
            return parsed
    return value


def decode_row(columns: dict[str, Any]) -> dict[str, Any]:
    return {k: decode_value(v) for k, v in columns.items()}
