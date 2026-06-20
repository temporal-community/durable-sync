"""Notion binding of the generic OAuth client (durable_sync.auth.oauth).

The OAuth 2.1 + PKCE + DCR flow is provider-agnostic and lives in auth/oauth.py;
this module just pins Notion's hosted MCP server and re-exports the flow so the
Notion bootstrap/prove/destination can keep importing `oauth.*`.
"""
from __future__ import annotations

from durable_sync.auth.oauth import (  # noqa: F401  (re-exported for callers)
    build_authorize_url,
    exchange_code,
    gen_pkce,
    new_state,
    refresh_access_token,
    register_client,
)
from durable_sync.auth import oauth as _generic

MCP_BASE = "https://mcp.notion.com"
MCP_ENDPOINT = f"{MCP_BASE}/mcp"  # Streamable HTTP transport


def discover() -> dict[str, str]:
    """Discover Notion's OAuth endpoints (generic flow, Notion base URL)."""
    return _generic.discover(MCP_BASE)
