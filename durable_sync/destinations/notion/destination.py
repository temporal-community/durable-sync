"""Reference Destination: Notion via the hosted MCP server.

Merges the two lineages:
  * clean neutral-Record encoding + paginated idempotent upsert (ex-devrel-demos),
  * Bearer-token transport (NO MCP SDK OAuthClientProvider), 429 backoff, and
    inter-write pacing (ex-devrel-ships).

Auth: the access token comes from `token_provider` (an async () -> str). The
default queries NotionAuthWorkflow, which owns the rotating refresh token; the
token never enters event history. We pass it as a plain `Authorization: Bearer`
header to the streamable-HTTP transport.

Property encoding quirks (live-server facts): dates expand to
`date:{prop}:start` (+ `:is_datetime`); multi-selects are JSON arrays (options
must pre-exist); checkboxes are `__YES__`/`__NO__`; a property literally named
`id`/`url` must be addressed `userDefined:{name}` (declare via
`user_defined_properties`).

Requires the `notion` extra:  pip install "durable-sync[notion]"
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, AsyncIterator

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from durable_sync.core import Record
from durable_sync.destinations.notion import oauth
from durable_sync.destinations.notion.token import current_access_token

_MAX_BODY = 50000          # cap page body length to keep create snappy
_MAX_429_RETRIES = 6
_BACKOFF_BASE_SECONDS = 1.0

# Optional hooks (app-supplied), kept out of the generic core:
TokenProvider = Callable[[], Awaitable[str]]
# Runs inside the open MCP session before each write — for DESTINATION-SIDE
# enrichment that must read Notion (e.g. resolving author handles to a relation).
# Gets the live session + the record; returns the (possibly mutated) record.
SessionEnrich = Callable[[ClientSession, Record], Awaitable[Record]]
# Maps a record to a page icon (emoji or URL), or None. Keeps Notion's icon
# concept off the neutral Record.
IconFor = Callable[[Record], "str | None"]


class NotionDestination:
    """Notion-MCP Destination. Configure with the target data source id and which
    property is the title / idempotency key / sync heartbeat."""

    name = "notion"

    def __init__(
        self,
        data_source_id: str,
        *,
        title_property: str = "Name",
        key_property: str = "Repo ID",
        synced_property: str | None = "Last synced",
        date_properties: set[str] | None = None,
        create_only_properties: set[str] | None = None,
        user_defined_properties: set[str] | None = None,
        token_provider: TokenProvider | None = None,
        session_enrich: SessionEnrich | None = None,
        icon_for: IconFor | None = None,
        pacing_seconds: float = 0.3,
    ):
        self.data_source_id = data_source_id
        self.title_property = title_property
        self.key_property = key_property
        self.synced_property = synced_property
        self.date_properties = date_properties or set()
        # Written only on CREATE (enrichment seeds): objective fields refresh every
        # run, but these are seeded once so human edits stick.
        self.create_only_properties = create_only_properties or set()
        # Property names that must be addressed as `userDefined:{name}` (Notion
        # reserves bare `id`/`url`). Ours deliberately avoid those, but a BYO
        # schema may need e.g. {"URL"}.
        self.user_defined_properties = user_defined_properties or set()
        self._token_provider = token_provider or current_access_token
        self._session_enrich = session_enrich
        self._icon_for = icon_for
        self.pacing_seconds = pacing_seconds

    @property
    def configured(self) -> bool:
        return bool(self.data_source_id)

    @property
    def config_hint(self) -> str:
        return "NOTION_DATA_SOURCE_ID unset"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_NotionSession"]:
        token = await self._token_provider()
        headers = {"Authorization": f"Bearer {token}"}
        async with streamablehttp_client(oauth.MCP_ENDPOINT, headers=headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield _NotionSession(session, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """True if err (or anything in its cause chain / ExceptionGroup) is an
        auth failure only a human can fix: the Bearer token was rejected and the
        refresh chain is broken (refresh token revoked/expired) -> re-bootstrap."""
        needles = ("401", "unauthorized", "invalid_token", "invalid_grant", "forbidden")
        seen: set[int] = set()
        stack: list[BaseException] = [err]
        while stack:
            cur = stack.pop()
            if id(cur) in seen:
                continue
            seen.add(id(cur))
            msg = str(cur).lower()
            if any(n in msg for n in needles):
                return True
            if isinstance(cur, BaseExceptionGroup):
                stack.extend(cur.exceptions)
            for nxt in (cur.__cause__, cur.__context__):
                if nxt is not None:
                    stack.append(nxt)
        return False


class _NotionSession:
    """One open MCP connection. Implements the DestinationSession protocol."""

    def __init__(self, session: ClientSession, destination: NotionDestination):
        self._session = session
        self._destination = destination

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        """Call an MCP tool; raise on error; return concatenated text content.

        Retries with exponential backoff on Notion's 429 (rate limit). MCP reports
        failures as isError results (NOT exceptions); without surfacing them, a
        failed write is silently counted a success -> missing rows. Raising lets
        Temporal retry (the upsert is idempotent, so a retry re-syncs safely)."""
        for attempt in range(_MAX_429_RETRIES):
            result = await self._session.call_tool(name, arguments)
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

    async def query_existing_ids(self) -> dict[str, str]:
        """{ key-property value -> page id } for rows already in the DB.

        Paginates LIMIT/OFFSET with ORDER BY the key property. Results cap at 100
        rows; unordered OFFSET reshuffles under concurrent edits and skips rows
        -> duplicates, so the ORDER BY is REQUIRED."""
        ds = self._destination.data_source_id
        key = self._destination.key_property
        PAGE = 100
        mapping: dict[str, str] = {}
        offset = 0
        while True:
            sql = (f'SELECT * FROM "collection://{ds}" '
                   f'ORDER BY "{key}" LIMIT {PAGE} OFFSET {offset}')
            raw = await self.call(
                "notion-query-data-sources",
                {"data": {"data_source_urls": [f"collection://{ds}"], "query": sql}},
            )
            rows = _rows_from_result(raw)
            for row in rows:
                kval = str(row.get(key) or "").strip()
                page_id = _page_id_from_row(row)
                if kval and page_id:
                    mapping[kval] = page_id
            if len(rows) < PAGE:
                break
            offset += PAGE
        return mapping

    async def create(self, record: Record, synced_at: dt.datetime) -> None:
        record = await self._maybe_enrich(record)
        page: dict[str, Any] = {"properties": self._encode(record.properties, synced_at)}
        if record.body:
            page["content"] = record.body[:_MAX_BODY]
        icon = self._icon(record)
        if icon:
            page["icon"] = icon
        await self.call(
            "notion-create-pages",
            {"parent": {"data_source_id": self._destination.data_source_id}, "pages": [page]},
        )
        await self._pace()

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> None:
        record = await self._maybe_enrich(record)
        # Skip create-only seeds (enrichment) so human edits to them survive;
        # refresh the rest. Page body is written on create, not refreshed.
        props = {
            k: v for k, v in record.properties.items()
            if k not in self._destination.create_only_properties
        }
        args: dict[str, Any] = {
            "page_id": existing_id,
            "command": "update_properties",
            "properties": self._encode(props, synced_at),
        }
        icon = self._icon(record)
        if icon:
            args["icon"] = icon
        await self.call("notion-update-page", args)
        await self._pace()

    async def _maybe_enrich(self, record: Record) -> Record:
        if self._destination._session_enrich is not None:
            return await self._destination._session_enrich(self._session, record)
        return record

    def _icon(self, record: Record) -> str | None:
        fn = self._destination._icon_for
        return fn(record) if fn else None

    async def _pace(self) -> None:
        # Stay under Notion's MCP rate limit (~few req/s). Backoff handles the
        # residual; this keeps us from hitting it in the first place.
        if self._destination.pacing_seconds > 0:
            await asyncio.sleep(self._destination.pacing_seconds)

    def _encode(self, properties: dict[str, Any], synced_at: dt.datetime) -> dict[str, Any]:
        """Neutral Python values -> Notion MCP wire format. bool is checked before
        int because bool subclasses int."""
        dest = self._destination
        out: dict[str, Any] = {}
        for name, val in properties.items():
            if val is None:
                continue
            if name in dest.date_properties:
                if val:
                    start, is_dt = _encode_date(val)
                    out[f"date:{name}:start"] = start
                    out[f"date:{name}:is_datetime"] = is_dt
            elif isinstance(val, bool):
                out[_key(name, dest)] = "__YES__" if val else "__NO__"
            elif isinstance(val, (int, float)):
                out[_key(name, dest)] = val
            elif isinstance(val, (list, tuple)):
                if val:  # multi-selects are JSON arrays; options must pre-exist
                    out[_key(name, dest)] = json.dumps(list(val))
            else:
                out[_key(name, dest)] = str(val)
        # Sync heartbeat: "Last synced" is a DATE column -> stamp the UTC date.
        if dest.synced_property:
            out[f"date:{dest.synced_property}:start"] = synced_at.date().isoformat()
            out[f"date:{dest.synced_property}:is_datetime"] = 0
        return out


def _key(name: str, dest: NotionDestination) -> str:
    """Prefix props that collide with Notion's reserved id/url addressing."""
    return f"userDefined:{name}" if name in dest.user_defined_properties else name


def _encode_date(val: Any) -> tuple[str, int]:
    """Return (start-string, is_datetime). A datetime, or an ISO string with a
    'T', carries time -> is_datetime=1; a plain date -> 0."""
    if isinstance(val, dt.datetime):
        return val.isoformat(), 1
    if isinstance(val, dt.date):
        return val.isoformat(), 0
    s = str(val)
    return s, (1 if "T" in s else 0)


# ---------------------------------------------------------------------------
# Result parsing (query results come back as JSON or, defensively, markdown)
# ---------------------------------------------------------------------------

def _rows_from_result(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _rows_from_markdown(raw)
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        for key in ("results", "rows", "data"):
            if isinstance(data.get(key), list):
                return [d for d in data[key] if isinstance(d, dict)]
    return []


def _rows_from_markdown(raw: str) -> list[dict[str, Any]]:
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


def _page_id_from_row(row: dict[str, Any]) -> str | None:
    for key in ("id", "page_id", "_id", "url", "page_url"):
        val = row.get(key)
        if isinstance(val, str) and val:
            return val.rsplit("/", 1)[-1].split("?")[0]
    return None
