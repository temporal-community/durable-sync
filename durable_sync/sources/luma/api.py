"""Luma API helpers — pure async HTTP + small pure transforms. No Temporal, no
config globals: every call takes its `headers`. Reusable from the Source's fetch
loop AND from an app's enrich hook (which gets the live client via the context).

Verify paths/params against Luma's docs as they evolve:
https://docs.luma.com/reference/get_v1-calendar-list-events
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from durable_sync.http import request_with_retry

BASE_URL = "https://public-api.luma.com/v1"
LIST_EVENTS_PATH = "/calendar/list-events"
GET_EVENT_PATH = "/event/get"
PAGE_LIMIT = 50
log = logging.getLogger("durable_sync.sources.luma")


def build_headers(api_key: str | None) -> dict[str, str]:
    return {"x-luma-api-key": api_key or "", "Accept": "application/json"}


async def list_event_entries(
    client: httpx.AsyncClient, headers: dict, after_iso: str, *, page_limit: int = PAGE_LIMIT
) -> list[dict[str, Any]]:
    """Raw Luma event entries on/after `after_iso`, paginating transparently.
    `list-events` does NOT include hosts — fetch those per event (see below)."""
    entries: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"after": after_iso, "pagination_limit": page_limit}
        if cursor:
            params["pagination_cursor"] = cursor
        r = await request_with_retry(
            client, "GET", f"{BASE_URL}{LIST_EVENTS_PATH}", headers=headers, params=params
        )
        r.raise_for_status()
        data = r.json()
        entries.extend(data.get("entries", data.get("events", [])))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return entries


async def get_event(client: httpx.AsyncClient, headers: dict, api_id: str) -> dict[str, Any] | None:
    """One event by id, as a list-style entry ({event, hosts, ...}) or None if
    gone. Used for targeted refreshes (only_items)."""
    if not api_id:
        return None
    r = await request_with_retry(
        client, "GET", f"{BASE_URL}{GET_EVENT_PATH}", headers=headers, params={"api_id": api_id}
    )
    if r.status_code == 404:
        log.warning("Luma event not found, skipping: %s", api_id)
        return None
    r.raise_for_status()
    return r.json()


async def get_event_hosts(client: httpx.AsyncClient, headers: dict, api_id: str) -> list[dict[str, Any]]:
    """Hosts for one event: [{name, email, ...}]. N+1 against the list (fine at
    current volume; gate behind a change-token if a source grows high-volume)."""
    if not api_id:
        return []
    r = await request_with_retry(
        client, "GET", f"{BASE_URL}{GET_EVENT_PATH}", headers=headers, params={"api_id": api_id}
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json().get("hosts", [])
