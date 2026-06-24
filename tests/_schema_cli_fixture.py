"""Wired Source + Destination fixture for the bootstrap_schema CLI test (offline)."""
from __future__ import annotations

from durable_sync.core import Record, SourceSpec
from tests.memory_destination import MemoryDestination


class _FixtureSource:
    name = "fixture"

    def specs(self) -> list[SourceSpec]:
        return [SourceSpec(key="unit", interval_minutes=60)]

    async def fetch(self, spec, only_items=None) -> list[Record]:
        return [
            Record(primary_key="1", properties={"Name": "Alpha", "Repo ID": "1",
                                                 "Stars": 5, "Topics": ["a", "b"]}),
            Record(primary_key="2", properties={"Name": "Beta", "Repo ID": "2",
                                                 "Stars": 9, "Topics": ["c"]}),
        ]


class _FixtureDest(MemoryDestination):
    # Expose the column roles the CLI reads as defaults.
    title_property = "Name"
    key_property = "Repo ID"
    synced_property = "Last synced"


SOURCE = _FixtureSource()
DESTINATION = _FixtureDest()
