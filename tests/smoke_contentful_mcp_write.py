"""Live write probe of Contentful over MCP (real OAuth — no CMA token).

⚠️ CREATES A REAL ENTRY (and publishes it if it gets that far) — delete it after.
Runs create -> get -> update -> publish via the ContentfulMcp client, printing the
RAW responses so we confirm the sys.id / version scrape against reality (the one
thing the schemas didn't tell us). This is the build->smoke->fix loop for the
MCP write path.

Pick a content type whose ONLY required field is the title (e.g. `card`):
    CONTENTFUL_SPACE_ID=...            (in .env)
    CONTENTFUL_SMOKE_CONTENT_TYPE=card
    CONTENTFUL_SMOKE_TITLE_FIELD=title   (default: title)
    PYTHONPATH=. .venv/bin/python tests/smoke_contentful_mcp_write.py
"""
from __future__ import annotations

import asyncio
import os

from durable_sync.env import load_env
from durable_sync.connectors.contentful import oauth, store
from durable_sync.connectors.contentful.mcp import open_contentful, entry_id, entry_version_of


async def _run(token: str, space: str, env: str, content_type: str, title_field: str, locale: str) -> None:
    async def token_provider() -> str:
        return token

    async with open_contentful(space, env, token_provider) as cf:
        fields = {title_field: {locale: "durable-sync MCP smoke (safe to delete)"}}

        print("=== create_entry (raw) ===")
        raw = await cf.call_raw("create_entry", {"contentTypeId": content_type, "fields": fields})
        print(raw[:3000])
        eid = entry_id(raw)
        print(f"\n>> scraped entry id: {eid!r}")
        if not eid:
            print("!! couldn't scrape sys.id — paste the raw above and I'll fix the parser.")
            return

        print("\n=== get_entry (raw) ===")
        graw = await cf.call_raw("get_entry", {"entryId": eid})
        print(graw[:2000])
        version = entry_version_of(graw)
        print(f"\n>> scraped version: {version!r}")
        if version is None:
            print("!! couldn't scrape sys.version — paste the raw above.")
            return

        print("\n=== update_entry ===")
        await cf.update_entry(eid, {title_field: {locale: "durable-sync MCP smoke (updated)"}}, version)
        print("updated OK")

        print("\n=== publish_entry ===")
        try:
            await cf.publish_entry(eid)
            print("published OK")
        except RuntimeError as e:
            if "publish_entry" in str(e) or "permission" in str(e).lower():
                print(f"publish gated by the MCP app installation (admin must enable publish_entry) — "
                      f"entry left as draft. This is NOT a failure: {str(e)[:160]}")
            else:
                raise
        print(f"\nMCP WRITE SMOKE PASS ✅ — create+update via MCP (publish per space config). "
              f"Entry id: {eid} (delete it).")


def main() -> None:
    load_env()
    space = os.environ.get("CONTENTFUL_SPACE_ID")
    content_type = os.environ.get("CONTENTFUL_SMOKE_CONTENT_TYPE")
    title_field = os.environ.get("CONTENTFUL_SMOKE_TITLE_FIELD", "title")
    env = os.environ.get("CONTENTFUL_ENVIRONMENT", "master")
    locale = os.environ.get("CONTENTFUL_DEFAULT_LOCALE", "en-US")
    if not (space and content_type):
        raise SystemExit("Set CONTENTFUL_SPACE_ID and CONTENTFUL_SMOKE_CONTENT_TYPE (e.g. card) in .env.")

    creds = store.load()
    if not creds:
        raise SystemExit("No credentials — run connectors.contentful.bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    if tokens.get("refresh_token"):
        creds["refresh_token"] = tokens["refresh_token"]
        store.save(creds)

    asyncio.run(_run(tokens["access_token"], space, env, content_type, title_field, locale))


if __name__ == "__main__":
    main()
