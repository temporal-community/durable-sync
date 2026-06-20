"""Live smoke of the LumaDestination WRITE path (real Luma API key).

⚠️  CREATES A REAL EVENT on the calendar tied to your LUMA_API_KEY (then updates
it). Luma's API has no general delete, so expect a leftover test event to tidy up
by hand. This is the first real exercise of the create/update payload shape —
if Luma rejects a field, the error here tells you what to fix in
connectors/luma/api.py / _encode_event.

    echo 'LUMA_API_KEY=...' >> /Users/webchick/durable-sync/.env
    PYTHONPATH=. python tests/smoke_luma_destination.py

Asserts: create returns an event id (recorded in the LinkStore), and a second
pass updates that same event in place (no duplicate) via the link.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from pathlib import Path

from durable_sync.core import Record
from durable_sync.connectors.luma import InMemoryLinkStore, LumaDestination


def _load_env() -> None:
    f = Path(__file__).resolve().parent.parent / ".env"
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


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
    _load_env()
    if not os.environ.get("LUMA_API_KEY"):
        raise SystemExit("LUMA_API_KEY not set — add it to .env first.")

    store = InMemoryLinkStore()   # one process: persists the link across the two passes
    dest = LumaDestination(link_store=store, title_property="Name", date_property="Date")
    now = dt.datetime.now(dt.timezone.utc)
    start = (now + dt.timedelta(days=30)).isoformat()

    rec = Record(primary_key="durable-sync-smoke-1",
                 properties={"Name": "durable-sync smoke event (safe to delete)", "Date": start})

    print("run 1 (create):", await _upsert(dest, [rec], now))
    print("  link store:", await store.get_all())

    rec.properties["Name"] = "durable-sync smoke event (updated)"
    stats2 = await _upsert(dest, [rec], now)
    print("run 2 (update):", stats2)

    assert stats2 == {"created": 0, "updated": 1, "existing_before": 1}, stats2
    print("\nLUMA DESTINATION SMOKE PASS ✅ — create + idempotent update via LinkStore.")


if __name__ == "__main__":
    asyncio.run(main())
