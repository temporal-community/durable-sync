"""Asana destination: direct REST + a self-serve Personal Access Token.

The second reference destination, deliberately different from Notion's MCP/OAuth:
plain REST, a PAT any user can mint (no admin, no workflow). If the Destination
protocol holds here too, it's neither transport- nor auth-shaped.

Requires the `asana` extra:  pip install "durable-sync[asana]"
"""
from __future__ import annotations

from durable_sync.connectors.asana.destination import AsanaDestination

__all__ = ["AsanaDestination"]
