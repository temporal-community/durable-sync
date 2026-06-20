"""Dump Contentful MCP usage + the input schemas of the tools we'll build against,
so the connector is written to the real shapes (not guessed).

    PYTHONPATH=. python -m durable_sync.connectors.contentful.describe

Prints get_initial_context (most MCP servers expect it first — it documents usage
and your space/environment) and the inputSchema of the read/write/schema tools.
"""
from __future__ import annotations

import asyncio
import json

from durable_sync.connectors.contentful import oauth, store
from durable_sync.transport.mcp import open_session

# The tools the source/destination/schema layers will use.
_KEY_TOOLS = [
    "search_entries", "get_entry", "resolve_entry_references",
    "create_entry", "update_entry", "publish_entry",
    "list_content_types", "get_content_type",
]


async def _describe(access_token: str) -> None:
    async def token_provider() -> str:
        return access_token
    async with open_session(oauth.MCP_ENDPOINT, token_provider) as session:
        print("=== get_initial_context ===")
        try:
            print(await session.call("get_initial_context", {}))
        except Exception as e:  # noqa: BLE001 — discovery, surface whatever it says
            print(f"(error calling get_initial_context: {e})")
        print()

        by_name = {t.name: t for t in await session.tools()}
        for name in _KEY_TOOLS:
            tool = by_name.get(name)
            if tool is None:
                print(f"=== {name}: NOT FOUND ===\n")
                continue
            print(f"=== {name} ===")
            if tool.description:
                print(tool.description.strip()[:400])
            print(json.dumps(tool.inputSchema, indent=2))
            print()


def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit("No credentials — run connectors.contentful.bootstrap first.")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    if tokens.get("refresh_token"):
        creds["refresh_token"] = tokens["refresh_token"]
        store.save(creds)
    asyncio.run(_describe(tokens["access_token"]))


if __name__ == "__main__":
    main()
