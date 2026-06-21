"""ListenBrainzDestination — mark records as "loved recordings" on ListenBrainz.

The write half lands favourites in an open, ISRC/MBID-native hub. Records are
keyed on ISRC (see SpotifySource); ListenBrainz feedback is keyed on a MusicBrainz
recording MBID, so the destination resolves `ISRC -> MBID` via MusicBrainz and
**caches that in a LinkStore** (resolution is rate-limited; the cache means we
resolve each ISRC once and survive restarts). MusicBrainz is the external mapping
DB; the LinkStore is our durable record of "ISRC X is loved as recording Y".

Idempotency: a LinkStore is REQUIRED (ListenBrainz exposes no place to stash our
ISRC, like Luma/Contentful). `query_existing_ids` returns the link map, so the
spine only `create`s ISRCs we haven't loved. "Loved" is binary, so `update` is a
no-op — and even a redundant submit is idempotent on ListenBrainz's side.

Requires the `listenbrainz` extra.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx

from durable_sync.core import Record, auth_error_in_chain
from durable_sync.linkstore import LinkStore
from durable_sync.connectors.listenbrainz import api
from durable_sync.connectors.listenbrainz.config import ListenBrainzConfig

log = logging.getLogger("durable_sync.connectors.listenbrainz")


class ListenBrainzDestination:
    name = "listenbrainz"

    def __init__(self, config: ListenBrainzConfig, *, link_store: LinkStore):
        self._config = config
        self.link_store = link_store              # REQUIRED — LB can't store our ISRC
        self.create_only_properties: set[str] = set()  # feedback carries no properties

    @property
    def configured(self) -> bool:
        return bool(os.environ.get(self._config.token_env) and self._config.user_name)

    @property
    def config_hint(self) -> str:
        return f"{self._config.token_env} unset or ListenBrainzConfig.user_name empty"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_ListenBrainzSession"]:
        token = os.environ.get(self._config.token_env, "")
        async with httpx.AsyncClient(timeout=30) as client:
            yield _ListenBrainzSession(client, self, token)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        """A rejected user token (401). Shared word-boundary matcher."""
        return auth_error_in_chain(err)


class _ListenBrainzSession:
    def __init__(self, client: httpx.AsyncClient, dest: ListenBrainzDestination, token: str):
        self._client = client
        self._d = dest
        self._token = token
        self._loved: set[str] | None = None  # lazily fetched once per session

    async def query_existing_ids(self) -> dict[str, str]:
        # The ISRC<->MBID correspondence lives in the app-owned store, not on LB.
        return await self._d.link_store.get_all()

    async def create(self, record: Record, synced_at: dt.datetime) -> bool:
        isrc = record.primary_key
        mbid = await self._resolve(isrc)
        if not mbid:
            log.warning("No MusicBrainz recording for ISRC %s (%r) — skipping",
                        isrc, record.properties.get("Name", ""))
            return False  # SKIPPED — unresolvable; tallied as skipped by the spine.

        loved = await self._loved_mbids()
        if mbid not in loved:                       # safety net vs the user's own loves
            await api.submit_feedback(self._client, self._token, mbid)
            loved.add(mbid)
        await self._d.link_store.put(isrc, mbid)    # remember so we resolve once
        return True

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> bool:
        # "Loved" is binary; nothing to refresh once the recording is loved.
        return False

    async def _resolve(self, isrc: str) -> str | None:
        cfg = self._d._config
        if cfg.mb_pacing_seconds > 0:               # respect MusicBrainz's ~1 req/sec
            await asyncio.sleep(cfg.mb_pacing_seconds)
        return await api.resolve_isrc_to_mbid(self._client, isrc, user_agent=cfg.user_agent)

    async def _loved_mbids(self) -> set[str]:
        if self._loved is None:
            self._loved = await api.get_loved_mbids(
                self._client, self._token, self._d._config.user_name)
        return self._loved
