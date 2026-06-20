"""Luma connector: a Luma calendar's events, BOTH directions.

`LumaSource` reads events -> Records; `LumaDestination` creates/updates events
(e.g. cross-posting from Notion), sharing api.py. Source policy (e.g. matching
hosts against your own directory) belongs in the source's `enrich` hook — see
LumaEventContext. Because Luma events can't hold a foreign key, the destination
takes a required `LinkStore` (app-owned correspondence; see the boundary doctrine).

Requires the `luma` extra:  pip install "durable-sync[luma]"
"""
from __future__ import annotations

from durable_sync.connectors.luma.destination import (
    InMemoryLinkStore,
    LinkStore,
    LumaDestination,
)
from durable_sync.connectors.luma.source import LumaConfig, LumaEventContext, LumaSource

__all__ = [
    "LumaSource", "LumaConfig", "LumaEventContext",
    "LumaDestination", "LinkStore", "InMemoryLinkStore",
]
