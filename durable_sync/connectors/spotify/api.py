"""Spotify Web API helpers — pure async HTTP + small pure transforms. No Temporal,
no config globals: every call takes its bearer `token`. All HTTP goes through
`http.request_with_retry` so 429/Retry-After backoff is inherited.

Docs: https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from durable_sync.core import DestinationHTTPError
from durable_sync.http import request_with_retry

BASE_URL = "https://api.spotify.com/v1"
SAVED_TRACKS_PATH = "/me/tracks"
PAGE_LIMIT = 50  # Spotify's max page size for this endpoint.
log = logging.getLogger("durable_sync.connectors.spotify")


def build_headers(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token or ''}", "Accept": "application/json"}


async def list_saved_tracks_page(
    client: httpx.AsyncClient, token: str, *, offset: int = 0, limit: int = PAGE_LIMIT
) -> tuple[list[dict[str, Any]], int | None]:
    """ONE page of the user's Liked Songs. Returns (items, next_offset) where each
    item is Spotify's saved-track wrapper ({added_at, track}) and next_offset is the
    offset to request next, or None when the library is exhausted. Offset paging is
    used (not the `next` URL) so the cursor the spine threads is a simple integer.

    Raises DestinationHTTPError on a non-2xx so the spine's auth/transient
    classification sees the status code (a 401 here means the access token lapsed)."""
    r = await request_with_retry(
        client, "GET", f"{BASE_URL}{SAVED_TRACKS_PATH}",
        headers=build_headers(token), params={"limit": limit, "offset": offset},
    )
    if r.status_code >= 400:
        raise DestinationHTTPError(
            r.status_code, f"Spotify GET {SAVED_TRACKS_PATH} -> {r.status_code}: {r.text[:600]}"
        )
    data = r.json()
    items = data.get("items", [])
    # `next` is null on the last page; otherwise advance by the page we just read.
    next_offset = offset + len(items) if data.get("next") else None
    return items, next_offset


def extract_isrc(track: dict[str, Any]) -> str:
    """The track's ISRC (International Standard Recording Code) — the cross-service
    identity used as the Record primary_key — or "" when absent (local files and
    some releases have none; the source drops those)."""
    return (track.get("external_ids") or {}).get("isrc", "") or ""


def artist_names(track: dict[str, Any]) -> list[str]:
    return [a.get("name", "") for a in track.get("artists", []) if a.get("name")]
