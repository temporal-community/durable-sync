"""SpotifySource — a user's Liked Songs ("favourites") -> neutral Records.

Each saved track maps to a content-style Record (see connectors/content.py),
keyed on its ISRC so a destination can dedupe ACROSS services (the Spotify track
id is meaningless to e.g. Apple Music; the ISRC is the shared identity). Tracks
with no ISRC are dropped + counted in a warning — never emit a record whose
primary_key can't dedupe.

Auth: the access token comes from `token_provider` (an async () -> str), default
the OAuthTokenWorkflow query binding in token.py — the same workflow-owned,
rotating-refresh-token pattern Notion/Contentful use. Set it up once:

    PYTHONPATH=. python -m durable_sync.connectors.spotify.bootstrap   # authorize as you
    PYTHONPATH=. python -m durable_sync.connectors.spotify.start       # hand token to the workflow

Requires the `spotify` extra.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

import httpx
from temporalio import activity

from durable_sync.core import Record, SourceSpec
from durable_sync.connectors import content
from durable_sync.connectors.spotify import api
from durable_sync.connectors.spotify.token import current_access_token

log = logging.getLogger("durable_sync.connectors.spotify")

# async () -> access token. Injectable so the source is testable without Temporal.
TokenProvider = Callable[[], Awaitable[str]]

_MAX_TEXT = 2000


@dataclass
class SpotifyConfig:
    """Everything Spotify-specific a deployment supplies. The client id + scopes
    live in oauth.py (bootstrap-time concerns); the running source only needs an
    access token, which it gets from the token_provider."""
    interval_minutes: int = 120       # 2h — favourites change slowly
    title_property: str = "Name"
    item_type: str = "Track"          # value written to the neutral "Type" column
    page_limit: int = api.PAGE_LIMIT


def _heartbeat(detail: str) -> None:
    """Heartbeat inside a Temporal activity; no-op otherwise so the source stays
    runnable/testable standalone."""
    if activity.in_activity():
        activity.heartbeat(detail)


class SpotifySource:
    name = "spotify"

    def __init__(
        self,
        config: SpotifyConfig | None = None,
        *,
        token_provider: TokenProvider | None = None,
    ):
        self._config = config or SpotifyConfig()
        self._token_provider = token_provider or current_access_token

    def specs(self) -> list[SourceSpec]:
        # One user's Liked Songs = a single unit of work / entity workflow.
        return [SourceSpec(key="liked", interval_minutes=self._config.interval_minutes)]

    async def fetch_page(
        self, spec: SourceSpec, only_items: list[str] | None, cursor: str | None
    ) -> tuple[list[Record], str | None]:
        """ONE page of Liked Songs + next cursor (None on the last page). The cursor
        is just Spotify's integer offset, packed as the spine's opaque string.

        `only_items` (a targeted refresh by ISRC) is not supported: the saved-tracks
        endpoint has no ISRC filter, so a targeted call would still page the whole
        library. The spine only passes only_items for signalled single-item syncs,
        which this source doesn't register; we ignore it and page normally."""
        cfg = self._config
        token = await self._token_provider()
        offset = 0 if cursor is None else content.unpack_cursor(cursor)["offset"]

        dropped = 0
        out: list[Record] = []
        async with httpx.AsyncClient(timeout=30) as client:
            items, next_offset = await api.list_saved_tracks_page(
                client, token, offset=offset, limit=cfg.page_limit
            )
            for item in items:
                track = item.get("track") or {}
                record = self._to_record(item)
                if record is None:
                    dropped += 1
                    continue
                out.append(record)
                _heartbeat(track.get("id", ""))

        if dropped:
            log.warning("Dropped %d Spotify track(s) with no ISRC (page offset=%d)", dropped, offset)
        next_cursor = content.pack_cursor(offset=next_offset) if next_offset is not None else None
        log.info("Fetched %d Spotify liked tracks for %s (offset=%d)", len(out), spec.key, offset)
        return out, next_cursor

    async def fetch(self, spec: SourceSpec, only_items: list[str] | None = None) -> list[Record]:
        """Whole library as one list — drains fetch_page (standalone/non-Temporal)."""
        records: list[Record] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.fetch_page(spec, only_items, cursor)
            records.extend(page)
            if cursor is None:
                return records

    def _to_record(self, item: dict) -> Record | None:
        """Map one saved-track wrapper ({added_at, track}) to a neutral Record, or
        None when the track has no ISRC (can't be a stable cross-service key). Pure
        (no IO)."""
        cfg = self._config
        track = item.get("track") or {}
        isrc = api.extract_isrc(track)
        if not isrc:
            return None

        names = api.artist_names(track)
        url = (track.get("external_urls") or {}).get("spotify")
        album = (track.get("album") or {}).get("name", "")

        return content.content_record(
            primary_key=isrc,
            title_property=cfg.title_property,
            title=track.get("name") or "(untitled track)",
            item_type=cfg.item_type,
            source="Spotify",
            url=url,
            date=item.get("added_at"),
            status="Published",
            author=", ".join(names),
            authors=names,
            extra={
                "Album": album[:_MAX_TEXT],
                "Spotify ID": track.get("id", ""),
            },
        )
