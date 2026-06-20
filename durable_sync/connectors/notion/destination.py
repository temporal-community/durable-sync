"""Reference Destination: Notion via the hosted MCP server.

Merges the two lineages:
  * clean neutral-Record encoding + paginated idempotent upsert (ex-devrel-demos),
  * Bearer-token transport (NO MCP SDK OAuthClientProvider), 429 backoff, and
    inter-write pacing (ex-devrel-ships).

Auth: the access token comes from `token_provider` (an async () -> str). The
default queries OAuthTokenWorkflow, which owns the rotating refresh token; the
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

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.connectors.notion import client as mcp
from durable_sync.connectors.notion.client import NotionMCP, TokenProvider
from durable_sync.connectors.notion.token import current_access_token

_MAX_BODY = 50000          # cap page body length to keep create snappy

# Optional hooks (app-supplied), kept out of the generic core:
# TokenProvider is imported from client.py (shared with the source).
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
        async with mcp.open_session(self._token_provider) as session:
            yield _NotionSession(session, self)

    # The worker auto-registers these so the token-owner workflow runs alongside
    # the sync. (Optional hook; destinations without aux work omit it.)
    def aux_workflows(self) -> list:
        from durable_sync.auth.oauth.workflow import OAuthTokenWorkflow
        return [OAuthTokenWorkflow]

    def aux_activities(self) -> list:
        from durable_sync.auth.oauth.refresh import refresh_oauth_token
        return [refresh_oauth_token]

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """A rejected Bearer token / broken refresh chain (revoked or expired) ->
        re-bootstrap. The default signatures (401/403, unauthorized, forbidden,
        invalid_token/grant) cover every Notion auth failure we've seen, so we
        delegate to the shared, word-boundary-correct matcher in the spine."""
        return auth_error_in_chain(err)


class _NotionSession:
    """One open MCP connection. Implements the DestinationSession protocol."""

    def __init__(self, session: NotionMCP, destination: NotionDestination):
        self._mcp = session
        self._destination = destination

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        return await self._mcp.call(name, arguments)

    async def query_existing_ids(self) -> dict[str, str]:
        """{ key-property value -> page id } for rows already in the DB.

        Paginates LIMIT/OFFSET with ORDER BY the key property; unordered OFFSET
        reshuffles under concurrent edits and skips rows -> duplicates, so the
        ORDER BY is REQUIRED."""
        ds = self._destination.data_source_id
        key = self._destination.key_property
        PAGE = 100
        mapping: dict[str, str] = {}
        offset = 0
        while True:
            sql = mcp.query_sql(ds, order_by=key, limit=PAGE, offset=offset)
            raw = await self.call(
                "notion-query-data-sources",
                {"data": {"data_source_urls": [f"collection://{ds}"], "query": sql}},
            )
            rows = mcp.rows_from_result(raw)
            for row in rows:
                kval = str(row.get(key) or "").strip()
                page_id = mcp.page_id_from_row(row)
                if kval and page_id:
                    mapping[kval] = page_id
            if len(rows) < PAGE:
                break
            offset += PAGE
        return mapping

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        record = await self._maybe_enrich(record)
        if record is None:
            return False  # session_enrich dropped it (out of scope)
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
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        record = await self._maybe_enrich(record)
        if record is None:
            return False  # session_enrich dropped it (out of scope)
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
        return True

    async def _maybe_enrich(self, record: Record) -> Record | None:
        """Run the destination-side enrich hook (if any). It may return None to
        DROP the record (an out-of-scope filter)."""
        if self._destination._session_enrich is not None:
            return await self._destination._session_enrich(self._mcp.session, record)
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
