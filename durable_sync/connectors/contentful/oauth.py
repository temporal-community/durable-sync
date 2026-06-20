"""Contentful binding of the generic OAuth client (durable_sync.auth.oauth).

Same flow as Notion — OAuth 2.1 + PKCE + dynamic client registration — just
pinned to Contentful's hosted MCP server. Confirmed via discover(): Contentful
exposes /authorize, /token, and /register (DCR), so no admin / no pre-registered
app is needed; you authorize as yourself (through your org's SSO, which is what
makes a static CFPAT unnecessary here).
"""
from __future__ import annotations

from durable_sync.auth.oauth.flow import (  # noqa: F401  (re-exported for callers)
    build_authorize_url,
    exchange_code,
    gen_pkce,
    new_state,
    refresh_access_token,
    register_client,
)
from durable_sync.auth.oauth import flow as _generic

MCP_BASE = "https://mcp.contentful.com"
MCP_ENDPOINT = f"{MCP_BASE}/mcp"  # Streamable HTTP transport


def discover() -> dict[str, str]:
    """Discover Contentful's OAuth endpoints (generic flow, Contentful base URL)."""
    return _generic.discover(MCP_BASE)
