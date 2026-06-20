"""Notion destination integration: MCP transport + OAuth-as-an-individual.

The reference destination for workspaces where REST is unavailable (PATs /
internal integration tokens locked to admins). Talks to Notion's hosted MCP
server, authorized as yourself via OAuth + dynamic client registration — no admin
needed. The rotating refresh token is owned by a Temporal workflow
(durable_sync.auth.oauth), not a file.

Requires the `notion` extra:  pip install "durable-sync[notion]"
"""
from __future__ import annotations

from durable_sync.connectors.notion.destination import NotionDestination

__all__ = ["NotionDestination"]

