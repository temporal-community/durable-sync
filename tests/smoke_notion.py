"""Live smoke of the Notion destination's upsert (real MCP, real OAuth token).

Mints an access token from the bootstrapped refresh token, creates a throwaway
Notion DB, upserts two rows, then re-runs to prove idempotency (no duplicates).
Uses a direct token_provider (no Temporal worker needed) to focus on the
destination's wire encoding + paginated upsert.

    PYTHONPATH=. python tests/smoke_notion.py   (after the bootstrap step)
"""
from __future__ import annotations

import asyncio
import datetime as dt
import re

from durable_sync.core import Record
from durable_sync.connectors.notion import oauth, store
from durable_sync.connectors.notion.destination import NotionDestination

DDL = ('CREATE TABLE ('
       '"Name" TITLE, "Repo ID" RICH_TEXT, "Stars" NUMBER, "Last synced" DATE)')


async def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit("No creds — run the bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    creds["refresh_token"] = tokens["refresh_token"]
    store.save(creds)                      # Notion rotates on every refresh
    access = tokens["access_token"]

    async def token_provider() -> str:
        return access

    dest = NotionDestination(
        "", title_property="Name", key_property="Repo ID",
        synced_property="Last synced", token_provider=token_provider,
    )

    # 1) create a throwaway DB via the destination's own session
    async with dest.connect() as s:
        res = await s.call("notion-create-database",
                           {"title": "DURABLE-SYNC LIVE TEST (throwaway — safe to trash)", "schema": DDL})
    ds = re.search(r"collection://([0-9a-f-]{32,36})", res).group(1)
    page = re.search(r"(https://www\.notion\.so/\S+|app\.notion\.com/\S+)", res)
    dest.data_source_id = ds
    print("created throwaway DB, data_source_id:", ds)
    if page:
        print("  open:", page.group(1).rstrip(").,"))

    records = [
        Record(primary_key="100", properties={"Name": "Alpha", "Repo ID": "100", "Stars": 5}),
        Record(primary_key="200", properties={"Name": "Beta", "Repo ID": "200", "Stars": 9}),
    ]
    now = dt.datetime.now(dt.timezone.utc)

    async def upsert(recs):
        created = updated = 0
        async with dest.connect() as s:
            existing = await s.query_existing_ids()
            for r in recs:
                if r.primary_key in existing:
                    await s.update(existing[r.primary_key], r, now); updated += 1
                else:
                    await s.create(r, now); created += 1
        return {"created": created, "updated": updated, "existing_before": len(existing)}

    print("run 1:", await upsert(records))
    records[0].properties["Stars"] = 42                 # refresh an objective field
    stats2 = await upsert(records)
    print("run 2:", stats2)

    assert stats2 == {"created": 0, "updated": 2, "existing_before": 2}, stats2
    print("\nNOTION LIVE SMOKE PASS ✅ — paginated idempotent upsert holds (no duplicates).")
    print("(Trash the throwaway DB in Notion when done.)")


if __name__ == "__main__":
    asyncio.run(main())
