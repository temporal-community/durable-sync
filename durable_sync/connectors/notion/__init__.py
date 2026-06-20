"""Notion connector: MCP transport + OAuth-as-an-individual, BOTH directions.

For workspaces where REST is unavailable (PATs / internal integration tokens
locked to admins). Talks to Notion's hosted MCP server, authorized as yourself via
OAuth + dynamic client registration — no admin needed. The rotating refresh token
is owned by a Temporal workflow (durable_sync.auth.oauth), not a file.

`NotionDestination` writes rows; `NotionSource` reads them — the read/write halves
share one MCP client + OAuth (see client.py). That shared transport is the whole
reason connectors are grouped by system.

Requires the `notion` extra:  pip install "durable-sync[notion]"
"""
from __future__ import annotations

from durable_sync.connectors.notion.destination import NotionDestination
from durable_sync.connectors.notion.source import NotionRowContext, NotionSource

__all__ = ["NotionDestination", "NotionSource", "NotionRowContext"]

