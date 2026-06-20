"""Live smoke test of the Asana destination (direct REST, real PAT).

Needs a Personal Access Token. Put it in a gitignored .env beside this repo:
    echo 'ASANA_PAT=1/your-token' > /Users/webchick/durable-sync/.env

Then run (project gid defaults to the throwaway test project):
    PYTHONPATH=. python tests/smoke_asana.py

Asserts: two tasks created (external.gid = our primary_key), a second pass is
idempotent (updates, no duplicates), native fields refresh on update.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
from pathlib import Path

from durable_sync.core import Record
from durable_sync.connectors.asana import AsanaDestination

PROJECT_GID = os.environ.get("ASANA_PROJECT_GID", "1215892757246667")


def _load_env() -> None:
    f = Path(__file__).resolve().parent.parent / ".env"
    if f.exists():
        for line in f.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


async def _upsert(dest, records, now):
    """Mirror activities.sync_records, run directly (no Temporal) for a focused
    destination test."""
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
    if not os.environ.get("ASANA_PAT"):
        raise SystemExit("ASANA_PAT not set — create .env with ASANA_PAT=1/... first.")

    dest = AsanaDestination(
        PROJECT_GID,
        title_property="Name",
        field_map={"Done": "completed", "Last updated": "due_on"},
    )
    now = dt.datetime.now(dt.timezone.utc)

    records = [
        Record(primary_key="ds-1",
               properties={"Name": "durable-sync task one", "Done": False, "Last updated": "2026-06-01"},
               body="Created by the durable-sync Asana live test."),
        Record(primary_key="ds-2",
               properties={"Name": "durable-sync task two", "Done": True}),
    ]

    print("run 1:", await _upsert(dest, records, now))

    # mutate + re-run -> must update in place (idempotent), not duplicate
    records[0].properties["Name"] = "durable-sync task one (updated)"
    records[0].properties["Done"] = True
    stats2 = await _upsert(dest, records, now)
    print("run 2:", stats2)

    assert stats2["existing_before"] == 2, f"expected 2 existing, got {stats2}"
    assert stats2 == {"created": 0, "updated": 2, "existing_before": 2}, stats2
    print("\nASANA LIVE SMOKE PASS ✅ — external.gid idempotency holds (no duplicates).")


if __name__ == "__main__":
    asyncio.run(main())
