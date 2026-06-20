"""Read-only probe of Contentful over MCP (real OAuth — no CMA token needed).

Safe: creates nothing. It (1) proves the MCP read path works headlessly, (2) lists
your content types — the ids the CMA-based introspect couldn't reach because the
CFPAT is SSO-blocked, and (3) prints raw tool responses so we can finalize the
connector's result parsing against real shapes (the one thing the schemas in
mega.json don't tell us).

    PYTHONPATH=. .venv/bin/python tests/smoke_contentful_mcp.py
    # optional: sample a search of one type
    CONTENTFUL_SMOKE_CONTENT_TYPE=blogPost PYTHONPATH=. .venv/bin/python tests/smoke_contentful_mcp.py
"""
from __future__ import annotations

import asyncio
import os

from durable_sync.connectors.contentful import oauth, store
from durable_sync.transport.mcp import open_session

ENV = os.environ.get("CONTENTFUL_ENVIRONMENT", "master")


async def _probe(access_token: str, space: str, content_type: str | None) -> None:
    async def token_provider() -> str:
        return access_token
    async with open_session(oauth.MCP_ENDPOINT, token_provider) as s:
        base = {"spaceId": space, "environmentId": ENV}

        # The server requires this first.
        await s.call("get_initial_context", {})

        print("=== list_content_types (raw) ===")
        print((await s.call("list_content_types", {**base, "limit": 10}))[:4000])
        print()

        if content_type:
            print(f"=== search_entries content_type={content_type} (raw) ===")
            print((await s.call("search_entries",
                                {**base, "query": {"content_type": content_type, "limit": 3}}))[:4000])
            print()


def main() -> None:
    space = os.environ.get("CONTENTFUL_SPACE_ID")
    if not space:
        raise SystemExit("Set CONTENTFUL_SPACE_ID (e.g. 0uuz8ydxyd9p) in .env.")
    creds = store.load()
    if not creds:
        raise SystemExit("No credentials — run connectors.contentful.bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    if tokens.get("refresh_token"):
        creds["refresh_token"] = tokens["refresh_token"]
        store.save(creds)

    asyncio.run(_probe(tokens["access_token"], space, os.environ.get("CONTENTFUL_SMOKE_CONTENT_TYPE")))
    print("CONTENTFUL MCP READ PROBE done ✅ — paste the raw output and we'll finalize parsing.")


if __name__ == "__main__":
    main()
