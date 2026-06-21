"""Example wiring: mirror your Spotify **Liked Songs** into a **Notion database**.

This is an *app* (the "pipeline.py" the docs refer to), not part of the library —
it just wires the shipped `SpotifySource` to the shipped `NotionDestination` and
runs the worker. Records are keyed on each track's ISRC, so re-runs update rows
instead of duplicating them, and the sync refreshes on its own timer forever.

It needs only free accounts — no paid/Apple Developer membership.

────────────────────────────────────────────────────────────────────────────
SETUP (run once)
────────────────────────────────────────────────────────────────────────────
1. Temporal dev server (separate terminal):
       temporal server start-dev

2. Authorize Spotify (PKCE, as yourself — create a free app at
   https://developer.spotify.com/dashboard and add the redirect URI it prints):
       SPOTIFY_CLIENT_ID=xxxx PYTHONPATH=. python -m durable_sync.connectors.spotify.bootstrap
       PYTHONPATH=. python -m durable_sync.connectors.spotify.start

3. Authorize Notion (as yourself — no admin token needed):
       PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap
       PYTHONPATH=. python -m durable_sync.connectors.notion.start

4. Create a Notion database and share it with your integration. Create a column
   for EVERY property the source emits (names must match exactly — the
   destination writes each non-empty property, and Notion rejects a write to a
   column that doesn't exist):
       Name         Title          (the track title)
       Source ID    Text           (the ISRC — the idempotency key)
       Type         Select/Text    ("Track")
       Source       Select/Text    ("Spotify")
       Status       Select/Text    ("Published")
       Author       Text           (artists, comma-joined)
       Authors      Multi-select   (one option per artist; auto-created on write)
       Album        Text
       URL          URL
       Date         Date           (when you liked it)
       Spotify ID   Text
       Last synced  Date           (sync heartbeat)
   Copy the database id (or its URL) into NOTION_DATA_SOURCE_ID below.

────────────────────────────────────────────────────────────────────────────
RUN
────────────────────────────────────────────────────────────────────────────
    NOTION_DATA_SOURCE_ID=<db-id-or-url> PYTHONPATH=. python examples/spotify_to_notion.py

Then trigger a sync immediately instead of waiting for the interval:
    temporal workflow signal --workflow-id "durable-sync:liked" --name sync_now --input '[]'
    temporal workflow query  --workflow-id "durable-sync:liked" --type status
"""
from __future__ import annotations

import asyncio
import os

from durable_sync.bootstrap import start_sources
from durable_sync.connectors.notion.destination import NotionDestination
from durable_sync.connectors.spotify.source import SpotifySource
from durable_sync.worker import run_worker


def build() -> tuple[SpotifySource, NotionDestination]:
    data_source = os.environ.get("NOTION_DATA_SOURCE_ID")
    if not data_source:
        raise SystemExit(
            "Set NOTION_DATA_SOURCE_ID to your Notion database id (or URL). "
            "See the setup steps at the top of this file."
        )

    source = SpotifySource()  # default config: Liked Songs, 2h interval
    destination = NotionDestination(
        data_source_id=data_source,
        title_property="Name",        # the Notion title column
        key_property="Source ID",     # where SpotifySource writes the ISRC primary key
        date_properties={"Date"},     # rendered as a Notion date, not text
        # "Last synced" (the default synced_property) is written every pass as a
        # heartbeat. Everything else refreshes each run; nothing is create-only here.
    )
    return source, destination


async def main() -> None:
    source, destination = build()
    await start_sources(source)            # one entity workflow per spec (idempotent)
    await run_worker(source, destination)  # host workflow + activities; runs forever


if __name__ == "__main__":
    asyncio.run(main())
