"""Luma source: events from a Luma calendar -> Records.

Ships the Luma *mechanism* (HTTP fetchers + neutral mapping); the *policy* (e.g.
matching hosts against your own directory of people) belongs in your app's
`enrich` hook — see LumaEventContext, which carries host emails for exactly that.

Requires the `luma` extra:  pip install "durable-sync[luma]"
"""
from __future__ import annotations

from durable_sync.sources.luma.source import LumaConfig, LumaEventContext, LumaSource

__all__ = ["LumaSource", "LumaConfig", "LumaEventContext"]
