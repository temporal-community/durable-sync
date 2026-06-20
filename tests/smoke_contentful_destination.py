"""Live smoke of the ContentfulDestination WRITE path (real CMA token + space).

⚠️  CREATES A REAL ENTRY (a draft) in your Contentful space, then updates it —
and publishes if CONTENTFUL_SMOKE_PUBLISH=1. Delete the test entry by hand after.
This is the first real exercise of the CMA create/update/version/publish flow;
if Contentful rejects the field-locale wrapping or the version header, the error
here tells you what to fix in connectors/contentful/api.py.

Set in .env (CMA token must be WRITE-capable):
    CONTENTFUL_SPACE_ID=...
    CONTENTFUL_CMA_TOKEN=...
    CONTENTFUL_SMOKE_CONTENT_TYPE=blogPost     # a content type in YOUR space
    CONTENTFUL_SMOKE_TITLE_FIELD=title         # its title field id (default: title)
    # CONTENTFUL_SMOKE_PUBLISH=1               # optional: also publish

    PYTHONPATH=. python tests/smoke_contentful_destination.py

Asserts: create returns an entry id (recorded in the LinkStore), and a second
pass updates that same entry in place (versioned, no duplicate) via the link.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os

from durable_sync.env import load_env
from durable_sync.core import Record
from durable_sync.linkstore import InMemoryLinkStore
from durable_sync.connectors.contentful import ContentfulDestination


async def _upsert(dest, records, now):
    created = updated = 0
    async with dest.connect() as s:
        existing = await s.query_existing_ids()
        for r in records:
            if r.primary_key in existing:
                await s.update(existing[r.primary_key], r, now)
                updated += 1
            else:
                await s.create(r, now)
                created += 1
    return {"created": created, "updated": updated, "existing_before": len(existing)}


async def main() -> None:
    load_env()
    space = os.environ.get("CONTENTFUL_SPACE_ID")
    content_type = os.environ.get("CONTENTFUL_SMOKE_CONTENT_TYPE")
    title_field = os.environ.get("CONTENTFUL_SMOKE_TITLE_FIELD", "title")
    if not (space and os.environ.get("CONTENTFUL_CMA_TOKEN") and content_type):
        raise SystemExit(
            "Set CONTENTFUL_SPACE_ID, CONTENTFUL_CMA_TOKEN, and "
            "CONTENTFUL_SMOKE_CONTENT_TYPE (a content type in your space) in .env."
        )

    store = InMemoryLinkStore()
    dest = ContentfulDestination(
        space_id=space,
        content_type=content_type,
        field_map={"Name": title_field},        # map the neutral title -> your title field id
        link_store=store,
        publish=os.environ.get("CONTENTFUL_SMOKE_PUBLISH") == "1",
    )
    now = dt.datetime.now(dt.timezone.utc)

    rec = Record(primary_key="durable-sync-smoke-1",
                 properties={"Name": "durable-sync smoke entry (safe to delete)"})

    print("run 1 (create):", await _upsert(dest, [rec], now))
    print("  link store:", await store.get_all())

    rec.properties["Name"] = "durable-sync smoke entry (updated)"
    stats2 = await _upsert(dest, [rec], now)
    print("run 2 (update):", stats2)

    assert stats2 == {"created": 0, "updated": 1, "existing_before": 1}, stats2
    print("\nCONTENTFUL DESTINATION SMOKE PASS ✅ — create + versioned update via LinkStore.")


if __name__ == "__main__":
    asyncio.run(main())
