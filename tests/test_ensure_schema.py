"""The Layer-1 -> Layer-2 seam, exercised offline (no network).

infer_schema (generic) -> Destination.ensure_schema (per-destination). Proves the
neutral Schema flows through a destination's hook, and that Notion's hook is
create-only (a no-op when a data source is already configured).
"""
from __future__ import annotations

import asyncio

from durable_sync.core import Record
from durable_sync.schema import Role, Schema, infer_schema
from tests.memory_destination import MemoryDestination


def _records():
    return [
        Record(primary_key="1", properties={"Name": "Alpha", "Repo ID": "1", "Stars": 5}),
        Record(primary_key="2", properties={"Name": "Beta", "Repo ID": "2", "Stars": 9}),
    ]


def test_infer_then_ensure_schema_memory_noop():
    schema = infer_schema(_records(), title="Name", key="Repo ID",
                          synced="Last synced", name="Repos")
    dest = MemoryDestination()
    result = asyncio.run(dest.ensure_schema(schema))
    assert result is None                       # dict store has no id to return
    assert dest.schema is schema                # spine drove the hook
    assert dest.schema.name == "Repos"
    assert dest.schema.title.name == "Name" and dest.schema.title.role is Role.TITLE


def test_notion_ensure_schema_is_create_only():
    # A configured Notion destination must NOT touch the network or re-create.
    from durable_sync.connectors.notion.destination import NotionDestination

    async def _tok() -> str:
        return "unused"

    dest = NotionDestination("already-configured-ds", token_provider=_tok)
    result = asyncio.run(dest.ensure_schema(Schema(columns=())))
    assert result is None
    assert dest.data_source_id == "already-configured-ds"  # untouched
