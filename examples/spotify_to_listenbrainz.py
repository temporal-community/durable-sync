"""Example wiring: mirror your Spotify **Liked Songs** into your ListenBrainz
**loved recordings**.

Both sides are free — Spotify (PKCE, authorize as yourself) and a ListenBrainz
user token. Each liked track is keyed on its ISRC; the destination resolves
ISRC -> MusicBrainz recording MBID (cached in a LinkStore so the rate-limited
lookup happens once) and submits "loved" feedback. Re-runs are idempotent.

────────────────────────────────────────────────────────────────────────────
SETUP (run once)
────────────────────────────────────────────────────────────────────────────
1. Temporal dev server (separate terminal):
       temporal server start-dev

2. Authorize Spotify (free app at https://developer.spotify.com/dashboard):
       SPOTIFY_CLIENT_ID=xxxx PYTHONPATH=. python -m durable_sync.connectors.spotify.bootstrap
       PYTHONPATH=. python -m durable_sync.connectors.spotify.start

3. Get a ListenBrainz user token from https://listenbrainz.org/settings/ and set:
       export LISTENBRAINZ_TOKEN=xxxx

────────────────────────────────────────────────────────────────────────────
RUN
────────────────────────────────────────────────────────────────────────────
    LISTENBRAINZ_TOKEN=xxxx PYTHONPATH=. python examples/spotify_to_listenbrainz.py YOUR_LB_USERNAME

Then trigger a sync immediately instead of waiting for the interval:
    temporal workflow signal --workflow-id "durable-sync:liked" --name sync_now --input '[]'

NOTE on the LinkStore: SqliteLinkStore below is a LOCAL-DEV store (one machine).
For a multi-worker / Temporal Cloud deployment, back the LinkStore with a shared
datastore (Postgres/Redis/…) — see durable_sync/linkstore.py.
"""
from __future__ import annotations

import asyncio
import sys

from durable_sync.bootstrap import start_sources
from durable_sync.linkstore import SqliteLinkStore
from durable_sync.connectors.listenbrainz.config import ListenBrainzConfig
from durable_sync.connectors.listenbrainz.destination import ListenBrainzDestination
from durable_sync.connectors.spotify.source import SpotifySource
from durable_sync.worker import run_worker


def build(user_name: str) -> tuple[SpotifySource, ListenBrainzDestination]:
    source = SpotifySource()  # default config: Liked Songs, 2h interval
    destination = ListenBrainzDestination(
        ListenBrainzConfig(user_name=user_name),
        # Durable cache of ISRC -> recording MBID, so the rate-limited MusicBrainz
        # lookup runs once per track and survives restarts.
        link_store=SqliteLinkStore("listenbrainz_links.db", route="spotify-liked"),
    )
    return source, destination


async def main(user_name: str) -> None:
    source, destination = build(user_name)
    await start_sources(source)            # one entity workflow per spec (idempotent)
    await run_worker(source, destination)  # host workflow + activities; runs forever


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python examples/spotify_to_listenbrainz.py YOUR_LB_USERNAME")
    asyncio.run(main(sys.argv[1]))
