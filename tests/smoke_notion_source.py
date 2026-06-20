"""Live smoke of the NotionSource READ path (real MCP, real OAuth token).

Read-only and safe — lists rows from a Notion data source and maps them to
Records, so you can eyeball what the query returns and confirm page-id keying +
column mapping against a real database.

    NOTION_SOURCE_DS=<data source id> PYTHONPATH=. python tests/smoke_notion_source.py
    (after the Notion bootstrap step — see connectors/notion/bootstrap.py)

The data source id is the collection UUID; reuse the throwaway DB from
smoke_notion.py, or point at a real one. Optionally set NOTION_SOURCE_ORDER to a
stable column for ordered pagination.
"""
from __future__ import annotations

import asyncio
import os

from durable_sync.connectors.notion import NotionSource, oauth, store


async def main() -> None:
    ds = os.environ.get("NOTION_SOURCE_DS")
    if not ds:
        raise SystemExit("Set NOTION_SOURCE_DS=<data source id> (the collection UUID).")

    creds = store.load()
    if not creds:
        raise SystemExit("No creds — run connectors.notion.bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    creds["refresh_token"] = tokens["refresh_token"]
    store.save(creds)                      # Notion rotates the refresh token on every use
    access = tokens["access_token"]

    async def token_provider() -> str:
        return access

    src = NotionSource(ds, order_property=os.environ.get("NOTION_SOURCE_ORDER"),
                       token_provider=token_provider)
    [spec] = src.specs()
    print("spec:", spec)
    records = await src.fetch(spec)
    print(f"\nfetched {len(records)} record(s); first few:")
    for r in records[:5]:
        print(f"\n  primary_key (page id): {r.primary_key}")
        for k, v in list(r.properties.items())[:8]:
            print(f"    {k}: {v!r}")

    assert all(r.primary_key for r in records), "every record must be keyed on a page id"
    print("\nNOTION SOURCE SMOKE PASS ✅ — rows -> Records keyed on page id.")


if __name__ == "__main__":
    asyncio.run(main())
