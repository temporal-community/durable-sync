"""Shared Notion-MCP transport — used by BOTH the destination (write) and the
source (read), since a system's two sides share a client + auth.

Owns: opening the streamable-HTTP MCP session with a Bearer token, the `call`
wrapper that turns MCP `isError` results into raised exceptions (with 429
backoff), and the pure parsers that turn a `notion-query-data-sources` result
into row dicts. No read/write policy lives here — that's in source.py /
destination.py.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from durable_sync.connectors.notion import oauth

_MAX_429_RETRIES = 6
_BACKOFF_BASE_SECONDS = 1.0

TokenProvider = Callable[[], Awaitable[str]]


class NotionMCP:
    """One open MCP connection. `.session` is the raw ClientSession (handed to a
    session_enrich hook); `.call` is the error-surfacing, 429-retrying tool call."""

    def __init__(self, session: ClientSession):
        self.session = session

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        """Call an MCP tool; raise on error; return concatenated text content.

        MCP reports failures as isError results (NOT exceptions); without surfacing
        them, a failed write is silently counted a success -> missing rows. Raising
        lets Temporal retry (sync is idempotent, so a retry re-syncs safely).
        Retries with exponential backoff on Notion's 429 (rate limit)."""
        for attempt in range(_MAX_429_RETRIES):
            result = await self.session.call_tool(name, arguments)
            payload = "\n".join(
                t for b in result.content if (t := getattr(b, "text", None))
            )
            if getattr(result, "isError", False):
                if "429" in payload and attempt < _MAX_429_RETRIES - 1:
                    await asyncio.sleep(_BACKOFF_BASE_SECONDS * (2 ** attempt))
                    continue
                raise RuntimeError(f"Notion MCP tool {name!r} returned an error: {payload[:600]}")
            return payload
        return ""  # unreachable: loop returns or raises


@asynccontextmanager
async def open_session(token_provider: TokenProvider) -> AsyncIterator[NotionMCP]:
    """Open an authenticated Notion-MCP session. `token_provider` yields a fresh
    access token (default: a query to the OAuthTokenWorkflow)."""
    token = await token_provider()
    headers = {"Authorization": f"Bearer {token}"}
    async with streamablehttp_client(oauth.MCP_ENDPOINT, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield NotionMCP(session)


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
