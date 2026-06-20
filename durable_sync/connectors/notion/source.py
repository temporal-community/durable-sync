"""NotionSource — read rows from a Notion data source -> Records.

The read half of the Notion connector; shares the MCP client + OAuth with
NotionDestination (see client.py), which is the whole reason connectors are
grouped by system. Each row becomes a Record keyed on its Notion page id — the
immutable, sync-safe id when Notion is the system of record.

Column values come back as the query renders them (text); for precise typing or
to pull page body content, use the `enrich` hook — it gets the raw row plus the
live MCP session for extra calls. Requires the `notion` extra.
"""
from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Union

from temporalio import activity

from durable_sync.core import Record, SourceSpec
from durable_sync.connectors.notion import client as mcp
from durable_sync.connectors.notion.client import NotionMCP, TokenProvider
from durable_sync.connectors.notion.token import current_access_token

log = logging.getLogger("durable_sync.connectors.notion.source")

EnrichHook = Callable[[Record, "NotionRowContext"], Union[Record, Awaitable[Record]]]

_PAGE = 100


@dataclass
class NotionRowContext:
    """Handed to the enrich hook: the raw queried row + the live MCP session, so
    enrich can type-coerce columns or fetch page content without re-connecting."""
    raw_row: dict
    session: NotionMCP


def _heartbeat(detail: str) -> None:
    if activity.in_activity():
        activity.heartbeat(detail)


class NotionSource:
    name = "notion"

    def __init__(
        self,
        data_source_id: str,
        *,
        order_property: str | None = None,
        interval_minutes: int = 30,
        token_provider: TokenProvider | None = None,
        enrich: EnrichHook | None = None,
        resolve_data_source: bool = True,
        decode: bool = True,
    ):
        # `data_source_id` may be a data source id OR a database id/URL — with
        # resolve_data_source on (default) the latter is resolved automatically.
        self.data_source_id = data_source_id
        # Pagination is LIMIT/OFFSET; ordering by a STABLE column keeps pages from
        # reshuffling under concurrent edits (else a run can skip/dupe rows — self-
        # corrects next run since the upsert is idempotent, but order if you can).
        self.order_property = order_property
        self.interval_minutes = interval_minutes
        self._token_provider = token_provider or current_access_token
        self._enrich = enrich
        self._resolve_ds = resolve_data_source
        self._decode = decode
        self._resolved_ds: str | None = None

    def specs(self) -> list[SourceSpec]:
        return [SourceSpec(key=f"ds:{self.data_source_id}", interval_minutes=self.interval_minutes,
                           params={"data_source_id": self.data_source_id})]

    # The Notion OAuth token workflow must run alongside ANY route that touches
    # Notion — source or destination. The worker registers a source's aux work too
    # (and dedupes, so a Notion->Notion route registers it once).
    def aux_workflows(self) -> list:
        from durable_sync.auth.oauth.workflow import OAuthTokenWorkflow
        return [OAuthTokenWorkflow]

    def aux_activities(self) -> list:
        from durable_sync.auth.oauth.refresh import refresh_oauth_token
        return [refresh_oauth_token]

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        ds = spec.params.get("data_source_id", self.data_source_id)
        targeted = set(only_items or [])   # page ids for a targeted refresh
        out: list[Record] = []
        async with mcp.open_session(self._token_provider) as session:
            if self._resolve_ds:
                if self._resolved_ds is None:
                    self._resolved_ds = await mcp.resolve_data_source_id(session, ds)
                    if self._resolved_ds != ds:
                        log.info("Resolved database %s -> data source %s", ds, self._resolved_ds)
                ds = self._resolved_ds
            offset = 0
            while True:
                sql = mcp.query_sql(ds, order_by=self.order_property, limit=_PAGE, offset=offset)
                raw = await session.call(
                    "notion-query-data-sources",
                    {"data": {"data_source_urls": [f"collection://{ds}"], "query": sql}},
                )
                rows = mcp.rows_from_result(raw)
                for row in rows:
                    record = self._to_record(row)
                    if record is None:
                        continue
                    if targeted and record.primary_key not in targeted:
                        continue
                    if self._enrich is not None:
                        ctx = NotionRowContext(raw_row=row, session=session)
                        result = self._enrich(record, ctx)
                        record = await result if inspect.isawaitable(result) else result
                    out.append(record)
                    _heartbeat(record.primary_key)
                if len(rows) < _PAGE:
                    break
                offset += _PAGE
        log.info("Fetched %d Notion rows for %s", len(out), spec.key)
        return out

    def _to_record(self, row: dict) -> Record | None:
        """Map one queried Notion row to a neutral Record. Pure (no IO). Returns
        None for a row with no resolvable page id — it can't be keyed idempotently
        (primary_key must be the immutable page id, never a column value)."""
        page_id = mcp.page_id_from_row(row)
        if not page_id:
            return None
        columns = mcp.row_columns(row)
        if self._decode:
            columns = mcp.decode_row(columns)
        return Record(primary_key=page_id, properties=columns)
