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

from durable_sync.core import DestinationHTTPError
from durable_sync.http import request_with_retry

BASE_URL = "https://public-api.luma.com/v1"
LIST_EVENTS_PATH = "/calendar/list-events"
GET_EVENT_PATH = "/event/get"
PAGE_LIMIT = 50
log = logging.getLogger("durable_sync.connectors.luma")


def build_headers(api_key: str | None) -> dict[str, str]:
    return {"x-luma-api-key": api_key or "", "Accept": "application/json"}


async def list_event_entries_page(
    client: httpx.AsyncClient, headers: dict, after_iso: str, *,
    cursor: str | None = None, page_limit: int = PAGE_LIMIT,
) -> tuple[list[dict[str, Any]], str | None]:
    """ONE page of raw Luma event entries on/after `after_iso`. Returns
    (entries, next_cursor) where next_cursor is Luma's pagination_cursor for the
    next page, or None when there are no more — the cursor the spine threads
    through `LumaSource.fetch_page`. `list-events` does NOT include hosts."""
    params: dict[str, Any] = {"after": after_iso, "pagination_limit": page_limit}
    if cursor:
        params["pagination_cursor"] = cursor
    r = await request_with_retry(
        client, "GET", f"{BASE_URL}{LIST_EVENTS_PATH}", headers=headers, params=params
    )
    r.raise_for_status()
    data = r.json()
    entries = data.get("entries", data.get("events", []))
    next_cursor = data.get("next_cursor") if data.get("has_more") else None
    return entries, next_cursor


async def list_event_entries(
    client: httpx.AsyncClient, headers: dict, after_iso: str, *, page_limit: int = PAGE_LIMIT
) -> list[dict[str, Any]]:
    """All raw Luma event entries on/after `after_iso` — drains
    list_event_entries_page. For non-Temporal callers; the spine pages directly."""
    entries: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        batch, cursor = await list_event_entries_page(
            client, headers, after_iso, cursor=cursor, page_limit=page_limit)
        entries.extend(batch)
        if cursor is None:
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


# --- write side (used by LumaDestination) -----------------------------------
# Verify paths/payload keys against Luma's docs — the write API evolves:
# https://docs.luma.com/reference/post_v1-event-create

CREATE_EVENT_PATH = "/event/create"
UPDATE_EVENT_PATH = "/event/update"


async def _write(client: httpx.AsyncClient, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    """POST to Luma; raise with status text (so is_auth_error can classify a 401).
    The client carries the x-luma-api-key header (set in connect)."""
    r = await request_with_retry(client, "POST", f"{BASE_URL}{path}", json=payload)
    if r.status_code >= 400:
        raise DestinationHTTPError(r.status_code, f"Luma POST {path} -> {r.status_code}: {r.text[:600]}")
    return r.json() if r.content else {}


async def create_event(client: httpx.AsyncClient, payload: dict[str, Any]) -> str:
    """Create an event; return its api_id."""
    data = await _write(client, CREATE_EVENT_PATH, payload)
    event = data.get("event", data)
    return event.get("api_id") or data.get("api_id") or ""


async def update_event(client: httpx.AsyncClient, api_id: str, payload: dict[str, Any]) -> None:
    """Update an existing event in place. NB: /event/update names the identifier
    `event_id` (create returns it as `api_id`) — confirmed against the live API."""
    await _write(client, UPDATE_EVENT_PATH, {"event_id": api_id, **payload})
