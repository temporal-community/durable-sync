"""Unit tests for the spine's idempotent upsert guard (activities.sync_records).

Covers the primary_key hardening: empty keys can't be idempotent (drop them) and
an in-batch duplicate key must not double-create (collapse, last-wins). Runs the
activity offline via Temporal's ActivityEnvironment (so activity.heartbeat works)
against the network-free MemoryDestination.
"""
from __future__ import annotations

import asyncio

from temporalio.testing import ActivityEnvironment

from durable_sync.activities import make_activities
from durable_sync.core import Record, SourceSpec
from tests.memory_destination import MemoryDestination


class _NullSource:
    name = "null"

    def specs(self) -> list[SourceSpec]:
        return []

    async def fetch(self, spec, only_items=None):
        return []


def _run(dest, records):
    _fetch, sync = make_activities(_NullSource(), dest)
    return asyncio.run(ActivityEnvironment().run(sync, records))


def test_empty_primary_key_is_skipped_not_written():
    dest = MemoryDestination()
    stats = _run(dest, [
        Record(primary_key="", properties={"Name": "no-key"}),
        Record(primary_key="ok", properties={"Name": "has-key"}),
    ])
    assert stats == {"total": 2, "created": 1, "updated": 0, "skipped": 1}
    assert set(dest.store) == {"ok"}  # the keyless record was NOT written


def test_in_batch_duplicate_key_collapses_last_wins():
    dest = MemoryDestination()
    stats = _run(dest, [
        Record(primary_key="dup", properties={"Name": "first"}),
        Record(primary_key="dup", properties={"Name": "second"}),
    ])
    # One create, one collapsed into skipped — NOT two creates of the same key.
    assert stats == {"total": 2, "created": 1, "updated": 0, "skipped": 1}
    assert list(dest.store) == ["dup"]
    assert dest.store["dup"]["properties"]["Name"] == "second"  # last-wins
    assert dest.store["dup"]["writes"] == 1  # written exactly once (no double-create)


def test_second_pass_updates_not_recreates():
    dest = MemoryDestination()
    recs = [Record(primary_key="a", properties={"Name": "A"})]
    _run(dest, recs)
    stats = _run(dest, recs)
    assert stats == {"total": 1, "created": 0, "updated": 1, "skipped": 0}
    assert dest.store["a"]["writes"] == 2
