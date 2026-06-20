"""Unit tests for Route + field-ownership (restrict_to_owned / compose). No network."""
from __future__ import annotations

import asyncio

from durable_sync.core import Record
from durable_sync.route import Route, compose, restrict_to_owned


def _rec():
    return Record(primary_key="x", properties={"Title": "T", "Tags": ["a"], "RSVP": 5})


def test_restrict_to_owned_drops_unowned():
    t = restrict_to_owned({"Title", "Tags"})
    out = t(_rec())
    assert out.properties == {"Title": "T", "Tags": ["a"]}   # RSVP (not owned) dropped
    assert out.primary_key == "x"                            # key untouched


def test_compose_runs_in_order_and_short_circuits():
    def derive(r):
        r.properties["Derived"] = 1
        return r
    def drop(r):
        return None
    composed = compose(derive, restrict_to_owned({"Title", "Derived"}))
    out = asyncio.run(composed(_rec()))
    assert out.properties == {"Title": "T", "Derived": 1}    # derived field survives the filter
    assert asyncio.run(compose(drop, derive)(_rec())) is None  # None short-circuits


def test_compose_awaits_async_stages():
    async def amark(r):
        r.properties["Async"] = True
        return r
    out = asyncio.run(compose(amark)(_rec()))
    assert out.properties["Async"] is True


def test_compose_empty_is_none():
    assert compose(None, None) is None


def test_route_build_transform_applies_ownership_after_transform():
    # transform derives a field; owns then restricts writes to {Title, Derived}.
    def derive(r):
        r.properties["Derived"] = "v"
        return r
    route = Route(source=object(), destination=object(), transform=derive, owns={"Title", "Derived"})
    out = asyncio.run(route.build_transform()(_rec()))
    assert out.properties == {"Title": "T", "Derived": "v"}


def test_route_no_ownership_passes_everything():
    route = Route(source=object(), destination=object())   # owns=None, transform=None
    assert route.build_transform() is None                 # nothing to do


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
