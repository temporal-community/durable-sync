"""LumaDestination — create/update Luma events from neutral Records.

The write half of the Luma connector (e.g. cross-posting events authored in
Notion). Shares api.py with LumaSource.

Idempotency, the interesting part: Luma events have **no field to stash a foreign
key in**, so — unlike Notion (key column) or Asana (`external.gid`) — this
destination can't recover "which Record maps to which event" from the event
itself. That correspondence has to live *outside*, so a `LinkStore` is **required**
(injected). Per the CONTRIBUTING boundary doctrine, the durable store is the app's
to own; the library defines the seam and ships only a dev-only in-memory impl.

Requires the `luma` extra.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol

import httpx

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.connectors.luma import api


class LinkStore(Protocol):
    """Durable map of source primary_key -> Luma event id. The app provides this
    (Luma can't hold the key itself); see the boundary doctrine in CONTRIBUTING.
    Both methods are async so an implementation can be DB- or workflow-backed."""

    async def get_all(self) -> dict[str, str]: ...
    async def put(self, primary_key: str, event_id: str) -> None: ...


class InMemoryLinkStore:
    """Dev/test only — NOT durable. Loses its map on restart, which for an FK-less
    destination means DUPLICATE events after a restart. Never use in production;
    provide a durable LinkStore (DB/Temporal-backed) instead."""

    def __init__(self) -> None:
        self._m: dict[str, str] = {}

    async def get_all(self) -> dict[str, str]:
        return dict(self._m)

    async def put(self, primary_key: str, event_id: str) -> None:
        self._m[primary_key] = event_id


class LumaDestination:
    name = "luma"

    def __init__(
        self,
        *,
        link_store: LinkStore,                 # REQUIRED — Luma can't store the FK itself
        token_env: str = "LUMA_API_KEY",
        title_property: str = "Name",
        date_property: str = "Date",
        timezone: str = "UTC",
        create_only_properties: set[str] | None = None,
        pacing_seconds: float = 0.0,
    ):
        self.link_store = link_store
        self.token_env = token_env
        self.title_property = title_property
        self.date_property = date_property
        self.timezone = timezone
        self.create_only_properties = create_only_properties or set()
        self.pacing_seconds = pacing_seconds

    @property
    def configured(self) -> bool:
        return bool(os.environ.get(self.token_env))

    @property
    def config_hint(self) -> str:
        return f"{self.token_env} unset"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_LumaSession"]:
        headers = api.build_headers(os.environ.get(self.token_env))
        async with httpx.AsyncClient(headers=headers, timeout=30) as client:
            yield _LumaSession(client, self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """A rejected API key (401). Shared word-boundary matcher."""
        return auth_error_in_chain(err)


class _LumaSession:
    def __init__(self, client: httpx.AsyncClient, dest: LumaDestination):
        self._client = client
        self._d = dest

    async def query_existing_ids(self) -> dict[str, str]:
        # The correspondence lives in the app-owned store, not on Luma's side.
        return await self._d.link_store.get_all()

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        event_id = await api.create_event(self._client, _encode_event(self._d, record))
        if event_id:
            await self._d.link_store.put(record.primary_key, event_id)
        await self._pace()
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        await api.update_event(self._client, existing_id, _encode_event(self._d, record, creating=False))
        await self._pace()
        return True

    async def _pace(self) -> None:
        if self._d.pacing_seconds > 0:
            await asyncio.sleep(self._d.pacing_seconds)


def _encode_event(dest: LumaDestination, record: Record, *, creating: bool = True) -> dict[str, Any]:
    """Neutral Record -> Luma event payload. Pure (no IO), so it's unit-testable.

    Minimal by design — title -> name, the date property -> start_at + timezone.
    Extend per your calendar's needs; unmapped Record properties are dropped (Luma
    has a fixed event schema, no arbitrary columns). On update, create-only
    properties are skipped so human edits in Luma survive."""
    props = record.properties
    payload: dict[str, Any] = {}

    name = props.get(dest.title_property)
    if name is not None:
        payload["name"] = str(name)

    start = props.get(dest.date_property)
    if start and not (not creating and dest.date_property in dest.create_only_properties):
        payload["start_at"] = str(start)
        payload["timezone"] = dest.timezone

    return payload
