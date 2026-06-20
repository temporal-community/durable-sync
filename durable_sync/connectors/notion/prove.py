"""Headless proof: NO browser. Loads the saved refresh token, mints a fresh
access token, and uses it to actually talk to the Notion MCP server.

    PYTHONPATH=. python -m durable_sync.connectors.notion.prove

The de-risking step for the whole architecture: if this works, the Temporal
auth workflow can do exactly the same on a timer with no human present.
"""
from __future__ import annotations

import asyncio

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from durable_sync.connectors.notion import oauth, store


async def _call_mcp(access_token: str) -> list[str]:
    """Connect to Notion MCP with the access token; return tool names. list_tools()
    succeeding proves the token authenticated the session."""
    headers = {"Authorization": f"Bearer {access_token}"}
    async with streamablehttp_client(oauth.MCP_ENDPOINT, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            return [t.name for t in tools.tools]


def main() -> None:
    creds = store.load()
    if not creds:
        raise SystemExit(
            f"No credentials at {store.path()}. Run the bootstrap first:\n"
            f"  PYTHONPATH=. python -m durable_sync.connectors.notion.bootstrap"
        )

    print("Refreshing access token (headless, no browser)...")
    tokens = oauth.refresh_access_token(
        creds["token_endpoint"], creds["client_id"], creds["refresh_token"]
    )
    # Notion ROTATES the refresh token on every use — persist the new one now,
    # atomically, or the next run fails with invalid_grant.
    creds["refresh_token"] = tokens["refresh_token"]
    store.save(creds)
    print(f"  Got access token (expires in {tokens.get('expires_in')}s). Rotated refresh token persisted.")

    print("Calling Notion MCP with the minted access token...")
    tool_names = asyncio.run(_call_mcp(tokens["access_token"]))
    print(f"\nSUCCESS — authenticated headlessly. MCP exposed {len(tool_names)} tools:")
    for name in tool_names:
        print(f"  - {name}")
    print("\nHeadless auth proven — the Temporal auth workflow can run this unattended.")


if __name__ == "__main__":
    main()
