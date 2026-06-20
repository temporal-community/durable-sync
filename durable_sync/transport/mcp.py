"""Generic Model Context Protocol transport (streamable-HTTP + Bearer token).

Provider-agnostic: open a session against ANY MCP endpoint with a token provider,
call tools (surfacing MCP's isError results as exceptions, with 429 backoff), and
list the available tools. Notion and Contentful both ride this — which is why it
lives here rather than inside one connector. No provider specifics (endpoints,
tool names, result parsing) live here.

Pairs with durable_sync.auth.oauth (the workflow-owned token) — but takes any
`token_provider`, so auth is fully decoupled.
"""
from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

_MAX_429_RETRIES = 6
_BACKOFF_BASE_SECONDS = 1.0
# Word-boundary match so a stray "429" inside an id/count/timestamp in the error
# body doesn't trigger a pointless backoff loop (same reasoning as the spine's
# word-boundary 401/403 auth matcher in core.py).
_RATE_LIMIT_RE = re.compile(r"\b429\b")

TokenProvider = Callable[[], Awaitable[str]]


class McpSession:
    """One open MCP connection. `.session` is the raw ClientSession (handed to
    enrich hooks); `.call` is the error-surfacing, 429-retrying tool call."""

    def __init__(self, session: ClientSession):
        self.session = session

    async def call(self, name: str, arguments: dict[str, Any]) -> str:
        """Call an MCP tool; raise on error; return concatenated text content.

        MCP reports failures as isError results (NOT exceptions); without surfacing
        them a failed write is silently counted a success -> missing rows. Raising
        lets Temporal retry (sync is idempotent, so a retry re-syncs safely).
        Retries with exponential backoff on a 429 (rate limit)."""
        for attempt in range(_MAX_429_RETRIES):
            result = await self.session.call_tool(name, arguments)
            payload = "\n".join(
                t for b in result.content if (t := getattr(b, "text", None))
            )
            if getattr(result, "isError", False):
                if _RATE_LIMIT_RE.search(payload) and attempt < _MAX_429_RETRIES - 1:
                    await asyncio.sleep(_BACKOFF_BASE_SECONDS * (2 ** attempt))
                    continue
                raise RuntimeError(f"MCP tool {name!r} returned an error: {payload[:600]}")
            return payload
        return ""  # unreachable: loop returns or raises

    async def tools(self) -> list:
        """The raw tool definitions this server exposes (name/description/inputSchema)."""
        return (await self.session.list_tools()).tools

    async def tool_names(self) -> list[str]:
        """The names of the tools this server exposes (handy for discovery)."""
        return [t.name for t in await self.tools()]


@asynccontextmanager
async def open_session(endpoint: str, token_provider: TokenProvider) -> AsyncIterator[McpSession]:
    """Open an authenticated MCP session against `endpoint`. `token_provider`
    yields a fresh access token (e.g. a query to the OAuthTokenWorkflow)."""
    token = await token_provider()
    headers = {"Authorization": f"Bearer {token}"}
    async with streamablehttp_client(endpoint, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield McpSession(session)
