"""Headless proof + tool discovery for Contentful's MCP server. NO browser.

Loads the saved refresh token, mints a fresh access token, opens the MCP session,
and lists the tools Contentful exposes — which is exactly what we need to build the
MCP source/destination (their tool names + schemas, instead of guessing).

    PYTHONPATH=. python -m durable_sync.connectors.contentful.prove

If this lists tools, the no-admin OAuth path works headlessly — the Temporal auth
workflow can mint tokens unattended, same as Notion.
"""
from __future__ import annotations

import asyncio

from durable_sync.connectors.contentful import oauth, store
from durable_sync.transport.mcp import open_session


async def _list_tools(access_token: str) -> list[str]:
    async def token_provider() -> str:
        return access_token
    async with open_session(oauth.MCP_ENDPOINT, token_provider) as session:
        return await session.tool_names()


def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit(
            f"No credentials at {store.path()}. Run the bootstrap first:\n"
            f"  PYTHONPATH=. python -m durable_sync.connectors.contentful.bootstrap"
        )

    print("Refreshing access token (headless, no browser)...")
    tokens = oauth.refresh_access_token(creds["token_endpoint"], creds["client_id"], creds["refresh_token"])
    # Persist a rotated refresh token now (providers rotate on every use).
    if tokens.get("refresh_token"):
        creds["refresh_token"] = tokens["refresh_token"]
        store.save(creds)
    print(f"  Got access token (expires in {tokens.get('expires_in')}s).\n")

    names = asyncio.run(_list_tools(tokens["access_token"]))
    print(f"SUCCESS — authenticated headlessly. Contentful MCP exposes {len(names)} tool(s):")
    for name in names:
        print(f"  - {name}")
    print("\nHeadless auth proven — paste this tool list and we'll build the connector against it.")


if __name__ == "__main__":
    main()
