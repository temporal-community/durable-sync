"""Unit tests for MultiSource: spec key-namespacing + fetch dispatch round-trip."""
from __future__ import annotations

import asyncio

import pytest

from durable_sync.core import Record, SourceSpec
from durable_sync.sources.multi import MultiSource


class _Fake:
    """Records the (key, only_items) it was fetched with, and returns one Record
    tagged with its own name so we can assert which source handled the call."""
    def __init__(self, name, specs):
        self.name = name
        self._specs = specs
        self.seen: list[tuple[str, list[str] | None]] = []

    def specs(self):
        return self._specs

    async def fetch(self, spec, only_items=None):
        self.seen.append((spec.key, only_items))
        return [Record(primary_key=f"{self.name}-1", properties={"by": self.name, "key": spec.key})]


def test_specs_namespaced_by_source_name():
    a = _Fake("luma", [SourceSpec(key="events", interval_minutes=10)])
    b = _Fake("youtube", [SourceSpec(key="channel:@temporalio", interval_minutes=20)])
    multi = MultiSource(a, b)
    by_key = {s.key: s for s in multi.specs()}
    assert set(by_key) == {"luma:events", "youtube:channel:@temporalio"}
    # interval + params are preserved through the namespacing.
    assert by_key["luma:events"].interval_minutes == 10
    assert by_key["youtube:channel:@temporalio"].interval_minutes == 20


def test_fetch_dispatches_to_owner_and_restores_inner_key():
    a = _Fake("luma", [SourceSpec(key="events")])
    # inner key itself contains a ':' — partition must keep it intact.
    b = _Fake("youtube", [SourceSpec(key="channel:@temporalio")])
    multi = MultiSource(a, b)

    recs = asyncio.run(multi.fetch(SourceSpec(key="youtube:channel:@temporalio"), ["vid1"]))
    assert recs[0].properties["by"] == "youtube"
    assert b.seen == [("channel:@temporalio", ["vid1"])]   # inner key restored
    assert a.seen == []                                     # other source untouched


def test_unknown_prefix_raises():
    multi = MultiSource(_Fake("luma", []))
    with pytest.raises(ValueError, match="no source named"):
        asyncio.run(multi.fetch(SourceSpec(key="ghost:thing")))


def test_rejects_duplicate_and_separator_names():
    with pytest.raises(ValueError, match="unique names"):
        MultiSource(_Fake("luma", []), _Fake("luma", []))
    with pytest.raises(ValueError, match="must not contain"):
        MultiSource(_Fake("bad:name", []))
    with pytest.raises(ValueError, match="at least one"):
        MultiSource()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-q"]))
