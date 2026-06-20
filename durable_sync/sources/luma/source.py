"""LumaSource — events from a Luma calendar, with a source-side enrichment hook.

Config is injected (no module globals), so the same code serves any calendar. The
base fetch produces a neutral Record per event. If you pass an `enrich` hook, the
source ALSO hands it a `LumaEventContext` (the raw entry + resolved hosts, incl.
emails, + the live client) so your app can layer on domain logic — e.g. resolve
hosts against your own directory of people — WITHOUT the source baking that policy in.

Auth: a Luma Plus API key (Calendar -> Settings -> Developer -> API keys), read
from the env var named by `LumaConfig.token_env`. Requires the `luma` extra.
"""
from __future__ import annotations

import inspect
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Union

import httpx
from temporalio import activity

from durable_sync.core import Record, SourceSpec
from durable_sync.sources import content
from durable_sync.sources.luma import api

log = logging.getLogger("durable_sync.sources.luma")

# enrich(record, ctx) -> Record (sync) or Awaitable[Record] (async); both ok.
EnrichHook = Callable[[Record, "LumaEventContext"], Union[Record, Awaitable[Record]]]


@dataclass
class LumaConfig:
    """Everything Luma-specific a deployment supplies."""
    token_env: str = "LUMA_API_KEY"
    lookback_days: int = 21          # rolling window pulled when no items are targeted
    interval_minutes: int = 360      # 6h
    title_property: str = "Name"
    item_type: str = "Event"         # value written to the neutral "Type" column


@dataclass
class LumaEventContext:
    """Handed to the enrich hook: everything already fetched for one event, plus
    the live client + headers so enrich can make extra calls without re-auth."""
    raw_entry: dict
    hosts: list[dict]              # [{name, email, ...}] — emails for identity matching
    client: httpx.AsyncClient
    headers: dict[str, str]


def _heartbeat(detail: str) -> None:
    """Heartbeat inside a Temporal activity; no-op otherwise, so the Source stays
    runnable/testable standalone."""
    if activity.in_activity():
        activity.heartbeat(detail)


class LumaSource:
    name = "luma"

    def __init__(self, config: LumaConfig | None = None, *, enrich: EnrichHook | None = None):
        self._config = config or LumaConfig()
        self._enrich = enrich

    def specs(self) -> list[SourceSpec]:
        # One calendar per API key -> a single unit of work / entity workflow.
        return [SourceSpec(key="events", interval_minutes=self._config.interval_minutes)]

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        cfg = self._config
        headers = api.build_headers(os.environ.get(cfg.token_env))

        async with httpx.AsyncClient(timeout=30) as client:
            if only_items:
                entries = [e for e in
                           [await api.get_event(client, headers, api_id) for api_id in only_items]
                           if e is not None]
            else:
                after_iso = (datetime.now(timezone.utc) - timedelta(days=cfg.lookback_days)).isoformat()
                entries = await api.list_event_entries(client, headers, after_iso)

            out: list[Record] = []
            for entry in entries:
                api_id = entry.get("api_id") or entry.get("event", {}).get("api_id") or ""
                hosts = await api.get_event_hosts(client, headers, api_id)
                record = self._to_record(entry, hosts)
                if self._enrich is not None:
                    ctx = LumaEventContext(raw_entry=entry, hosts=hosts, client=client, headers=headers)
                    result = self._enrich(record, ctx)
                    record = await result if inspect.isawaitable(result) else result
                out.append(record)
                _heartbeat(api_id)

        log.info("Fetched %d Luma events for %s", len(out), spec.key)
        return out

    def _to_record(self, entry: dict, hosts: list[dict]) -> Record:
        """Map one Luma entry (+ its hosts) to a neutral Record. Pure (no IO)."""
        cfg = self._config
        event = entry.get("event", entry)
        source_id = entry.get("api_id") or event.get("api_id") or ""
        name = event.get("name") or "(untitled event)"
        start_at = event.get("start_at")

        slug = event.get("url")
        url = slug if (slug or "").startswith("http") else (f"https://lu.ma/{slug}" if slug else None)

        host_names = [h.get("name", "") for h in hosts if h.get("name")]
        status = "Scheduled" if (start_at and _is_future(start_at)) else "Published"

        return content.content_record(
            primary_key=source_id,
            title_property=cfg.title_property,
            title=name,
            item_type=cfg.item_type,
            source="Luma",
            url=url,
            date=start_at,
            status=status,
            author=", ".join(host_names),
            authors=host_names,
        )


def _is_future(start_at: str) -> bool:
    try:
        return datetime.fromisoformat(start_at.replace("Z", "+00:00")) > datetime.now(timezone.utc)
    except ValueError:
        return False
