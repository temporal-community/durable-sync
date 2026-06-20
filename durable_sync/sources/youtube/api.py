"""YouTube Data API v3 helpers — pure async HTTP + small pure transforms. No
Temporal, no config globals. Lists a channel's "uploads" playlist (channel id or
@handle, resolved here), yielding {videoId, title, description, publishedAt,
viewCount}. Read-only.

Docs: https://developers.google.com/youtube/v3/docs
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from durable_sync.http import request_with_retry

API = "https://www.googleapis.com/youtube/v3"
PAGE = 50  # API max
log = logging.getLogger("durable_sync.sources.youtube")


async def _get(client: httpx.AsyncClient, api_key: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
    r = await request_with_retry(client, "GET", f"{API}/{path}", params={**params, "key": api_key})
    r.raise_for_status()
    return r.json()


def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


async def uploads_playlist(client: httpx.AsyncClient, api_key: str, channel: str) -> str:
    """Resolve a channel id ('UC…') or '@handle' to its uploads playlist id."""
    ch = channel.strip()
    if ch.startswith("UC") and " " not in ch:
        data = await _get(client, api_key, "channels", {"part": "contentDetails", "id": ch})
    else:
        data = await _get(client, api_key, "channels", {"part": "contentDetails", "forHandle": ch.lstrip("@")})
    items = data.get("items", [])
    if not items:
        raise RuntimeError(f"YouTube channel not found: {channel!r}")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


async def list_videos(client: httpx.AsyncClient, api_key: str, playlist: str, after_iso: str) -> list[dict[str, Any]]:
    """Videos published on/after `after_iso`, newest first (with view counts). The
    uploads playlist is reverse-chronological, so we stop once we pass the window."""
    after = parse_ts(after_iso)
    out: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params: dict[str, Any] = {"part": "snippet,contentDetails", "playlistId": playlist, "maxResults": PAGE}
        if page_token:
            params["pageToken"] = page_token
        data = await _get(client, api_key, "playlistItems", params)

        metas: list[dict[str, Any]] = []
        stop = False
        for it in data.get("items", []):
            sn, cd = it.get("snippet", {}), it.get("contentDetails", {})
            vid = cd.get("videoId")
            pub = cd.get("videoPublishedAt") or sn.get("publishedAt")
            if not vid or not pub:
                continue
            if parse_ts(pub) < after:
                stop = True
                break
            metas.append({"videoId": vid, "title": sn.get("title", ""),
                          "description": sn.get("description", ""), "publishedAt": pub})

        views = await view_counts(client, api_key, [m["videoId"] for m in metas]) if metas else {}
        for m in metas:
            m["viewCount"] = views.get(m["videoId"])
            out.append(m)

        page_token = None if stop else data.get("nextPageToken")
        if not page_token:
            break
    return out


async def videos_by_id(client: httpx.AsyncClient, api_key: str, ids: list[str]) -> list[dict[str, Any]]:
    """Specific videos by id (for targeted refreshes), same meta shape as the list."""
    ids = [i for i in ids if i]
    if not ids:
        return []
    data = await _get(client, api_key, "videos", {"part": "snippet,statistics", "id": ",".join(ids[:PAGE])})
    out: list[dict[str, Any]] = []
    for it in data.get("items", []):
        sn, st = it.get("snippet", {}), it.get("statistics", {})
        vc = st.get("viewCount")
        out.append({
            "videoId": it.get("id", ""),
            "title": sn.get("title", ""),
            "description": sn.get("description", ""),
            "publishedAt": sn.get("publishedAt"),
            "viewCount": int(vc) if vc is not None else None,
        })
    return out


async def view_counts(client: httpx.AsyncClient, api_key: str, ids: list[str]) -> dict[str, int | None]:
    """viewCount per video id (one batched call; ids already <= PAGE)."""
    if not ids:
        return {}
    data = await _get(client, api_key, "videos", {"part": "statistics", "id": ",".join(ids)})
    out: dict[str, int | None] = {}
    for it in data.get("items", []):
        vc = it.get("statistics", {}).get("viewCount")
        out[it["id"]] = int(vc) if vc is not None else None
    return out
