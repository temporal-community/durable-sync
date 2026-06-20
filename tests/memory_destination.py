"""In-memory Destination — conformance fixture + offline test target.

Implements the full Destination/DestinationSession protocol against a plain dict.
Proves the protocol isn't Notion-shaped (no MCP, no OAuth, no network) and lets
the whole spine be exercised offline. Also a worked example of a minimal
destination: connect / query_existing_ids / create / update / is_auth_error.
"""
from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from durable_sync.core import Record


class MemoryDestination:
    name = "memory"

    def __init__(self, *, create_only_properties: set[str] | None = None):
        # primary_key -> {"properties": {...}, "synced_at": iso, "writes": n}
        self.store: dict[str, dict[str, Any]] = {}
        self.create_only_properties = create_only_properties or set()

    configured = True
    config_hint = "(memory destination is always configured)"

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["_MemorySession"]:
        yield _MemorySession(self)

    @staticmethod
    def is_auth_error(err: BaseException) -> bool:
        return False  # no interactive auth


class _MemorySession:
    def __init__(self, dest: MemoryDestination):
        self._d = dest

    async def query_existing_ids(self) -> dict[str, str]:
        # destination-internal id == primary_key for this trivial store
        return {pk: pk for pk in self._d.store}

    async def create(self, record: Record, synced_at: dt.datetime) -> None:
        self._d.store[record.primary_key] = {
            "properties": dict(record.properties),
            "synced_at": synced_at.isoformat(),
            "writes": 1,
        }

    async def update(self, existing_id: str, record: Record, synced_at: dt.datetime) -> None:
        row = self._d.store[existing_id]
        for k, v in record.properties.items():
            if k not in self._d.create_only_properties:  # honor create-only seeds
                row["properties"][k] = v
        row["synced_at"] = synced_at.isoformat()
        row["writes"] = row.get("writes", 1) + 1
