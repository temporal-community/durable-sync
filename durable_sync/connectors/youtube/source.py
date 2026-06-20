"""YouTubeSource — a channel's uploads -> Records, with a source-side enrich hook.

YouTube has no per-video author field. The base Record therefore leaves "Author"
empty and stashes the title + description as a "Scan Text" property (and on the
enrich context) so an app that needs attribution can scan it for known names —
an "inverted match" — rather than relying on a structured author. That policy
lives in your `enrich` hook, not here.

Auth: a YouTube Data API v3 key, read from the env var named by
`YouTubeConfig.token_env`. Requires the `youtube` extra.
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
from durable_sync.connectors import content
from durable_sync.connectors.youtube import api

log = logging.getLogger("durable_sync.connectors.youtube")

EnrichHook = Callable[[Record, "YouTubeVideoContext"], Union[Record, Awaitable[Record]]]

_MAX_SUMMARY = 2000


@dataclass
class YouTubeConfig:
    """Everything YouTube-specific a deployment supplies. `channel` (required) is a
    channel id ('UC…') or a handle ('@name'); the client resolves a handle to its
    id."""
    channel: str
    token_env: str = "YOUTUBE_API_KEY"
    lookback_days: int = 21
    interval_minutes: int = 360
    title_property: str = "Name"
    item_type: str = "Video"         # value written to the neutral "Type" column


@dataclass
class YouTubeVideoContext:
    """Handed to the enrich hook: the raw video meta (incl. full description) plus
    the live client, so enrich can do inverted name-matching or extra lookups."""
    raw_video: dict
    scan_text: str                 # title + description, for inverted matching
    client: httpx.AsyncClient
    api_key: str


def _heartbeat(detail: str) -> None:
    if activity.in_activity():
        activity.heartbeat(detail)


class YouTubeSource:
    name = "youtube"

    def __init__(self, config: YouTubeConfig, *, enrich: EnrichHook | None = None):
        self._config = config
        self._enrich = enrich

    def specs(self) -> list[SourceSpec]:
        ch = self._config.channel
        return [SourceSpec(key=f"channel:{ch}", interval_minutes=self._config.interval_minutes,
                           params={"channel": ch})]

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        cfg = self._config
        api_key = os.environ.get(cfg.token_env, "")
        channel = spec.params.get("channel", cfg.channel)

        async with httpx.AsyncClient(timeout=30) as client:
            if only_items:
                videos = await api.videos_by_id(client, api_key, only_items)
            else:
                after_iso = (datetime.now(timezone.utc) - timedelta(days=cfg.lookback_days)).isoformat()
                playlist = await api.uploads_playlist(client, api_key, channel)
                videos = await api.list_videos(client, api_key, playlist, after_iso)

            out: list[Record] = []
            for v in videos:
                record = self._to_record(v)
                if self._enrich is not None:
                    title = v.get("title") or ""
                    scan_text = f"{title}\n{v.get('description', '')}"
                    ctx = YouTubeVideoContext(raw_video=v, scan_text=scan_text, client=client, api_key=api_key)
                    result = self._enrich(record, ctx)
                    record = await result if inspect.isawaitable(result) else result
                out.append(record)
                _heartbeat(v.get("videoId", ""))

        log.info("Fetched %d YouTube videos for %s", len(out), spec.key)
        return out

    def _to_record(self, v: dict) -> Record:
        """Map one video to a neutral Record. Pure (no IO)."""
        cfg = self._config
        vid = v.get("videoId", "")
        title = v.get("title") or "(untitled video)"
        description = v.get("description") or ""
        return content.content_record(
            primary_key=vid,
            title_property=cfg.title_property,
            title=title,
            item_type=cfg.item_type,
            source="YouTube",
            url=f"https://www.youtube.com/watch?v={vid}" if vid else None,
            date=v.get("publishedAt"),
            status="Published",
            author="",                          # no per-video author on YouTube
            extra={
                "Reach": v.get("viewCount"),
                "Summary": description[:_MAX_SUMMARY],
                # Free text for inverted matching by an app's enrich/transform hook.
                "Scan Text": f"{title}\n{description}"[:_MAX_SUMMARY],
            },
        )
