"""Contentful over its hosted MCP server (mcp.contentful.com) — the no-admin / SSO
path, when a static CMA token is blocked and the MCP-OAuth token doesn't work
against the REST API.

Contentful's MCP is AGENT-oriented: tools take clean JSON but return LLM-formatted
pseudo-XML (with prose prefixes, arrays as repeated elements). So:
  * WRITES are reliable — inputs are clean JSON (fields = {fieldId:{locale:value}},
    same shape as the REST encoder), and we only scrape two scalars from responses:
    the new entry's sys.id (create) and sys.version (for the optimistic-lock update).
  * READS over MCP are fragile (multi-entry XML) — prefer the REST source when you
    have CMA access; this module is the write path.

Pairs the generic MCP transport (durable_sync.transport.mcp) with the OAuth binding
(connectors.contentful.oauth). get_initial_context is called once on open (the
server requires it first).
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from durable_sync.transport.mcp import TokenProvider, open_session as _open_session
from durable_sync.connectors.contentful import oauth


class ContentfulMcp:
    """Write-oriented wrapper over an open Contentful-MCP session."""

    def __init__(self, session, space_id: str, environment: str):
        self._s = session
        self.space_id = space_id
        self.environment = environment

    def _base(self) -> dict[str, str]:
        return {"spaceId": self.space_id, "environmentId": self.environment}

    async def call_raw(self, tool: str, args: dict[str, Any]) -> str:
        """Escape hatch for discovery/smokes: call a tool, return the raw text."""
        return await self._s.call(tool, {**self._base(), **args})

    async def create_entry(self, content_type: str, fields: dict[str, Any]) -> str | None:
        """Create an entry; return its new sys.id (None if it couldn't be scraped)."""
        raw = await self._s.call(
            "create_entry", {**self._base(), "contentTypeId": content_type, "fields": fields}
        )
        return entry_id(raw)

    async def entry_version(self, entry_id_: str) -> int | None:
        """sys.version of an entry (required for the optimistic-lock update)."""
        raw = await self._s.call("get_entry", {**self._base(), "entryId": entry_id_})
        return entry_version_of(raw)

    async def update_entry(self, entry_id_: str, fields: dict[str, Any], version: int) -> None:
        await self._s.call(
            "update_entry",
            {**self._base(), "entryId": entry_id_, "version": version, "fields": fields},
        )

    async def publish_entry(self, entry_id_: str) -> None:
        await self._s.call("publish_entry", {**self._base(), "entryId": [entry_id_]})


@asynccontextmanager
async def open_contentful(
    space_id: str, environment: str, token_provider: TokenProvider
) -> AsyncIterator[ContentfulMcp]:
    """Open a Contentful-MCP session (calls get_initial_context first, as required)."""
    async with _open_session(oauth.MCP_ENDPOINT, token_provider) as session:
        cf = ContentfulMcp(session, space_id, environment)
        await session.call("get_initial_context", {})
        yield cf


# --- response scraping ------------------------------------------------------
# Contentful's MCP returns prose-prefixed pseudo-XML. We only need two scalars from
# entity responses: sys.id and sys.version. We strip the prose, wrap to tolerate
# multiple roots / stray content, and take the FIRST <sys> that has a direct <id>
# child — that's the entity's own sys (nested space/environment sys come later in
# document order). Regex fallback if the XML won't parse.

def _entity_sys(raw: str) -> ET.Element | None:
    start = raw.find("<")
    if start < 0:
        return None
    try:
        root = ET.fromstring(f"<root>{raw[start:]}</root>")
    except ET.ParseError:
        return None
    for sys in root.iter("sys"):
        if sys.find("id") is not None:   # direct <id> child => the entity's own sys
            return sys
    return None


def entry_id(raw: str) -> str | None:
    sys = _entity_sys(raw)
    if sys is not None:
        el = sys.find("id")
        if el is not None and el.text:
            return el.text.strip()
    m = re.search(r"<id>\s*([^<\s]+)\s*</id>", raw)  # best-effort fallback
    return m.group(1) if m else None


def entry_version_of(raw: str) -> int | None:
    sys = _entity_sys(raw)
    if sys is not None:
        el = sys.find("version")
        if el is not None and (el.text or "").strip().isdigit():
            return int(el.text.strip())
    m = re.search(r"<version>\s*(\d+)\s*</version>", raw)
    return int(m.group(1)) if m else None
