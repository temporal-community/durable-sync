"""Unit tests for the link stores (idempotency correspondence). No network."""
from __future__ import annotations

import asyncio

from durable_sync.linkstore import InMemoryLinkStore, LinkStore, SqliteLinkStore


def test_inmemory_roundtrip():
    s = InMemoryLinkStore()
    assert asyncio.run(s.get_all()) == {}
    asyncio.run(s.put("pk1", "dest1"))
    asyncio.run(s.put("pk2", "dest2"))
    assert asyncio.run(s.get_all()) == {"pk1": "dest1", "pk2": "dest2"}


def test_sqlite_is_durable_across_instances(tmp_path):
    db = str(tmp_path / "links.db")
    s1 = SqliteLinkStore(db, route="notion->luma")
    asyncio.run(s1.put("pk1", "evt1"))
    # a fresh instance (≈ a restart) still sees it — the FK-less idempotency fix.
    s2 = SqliteLinkStore(db, route="notion->luma")
    assert asyncio.run(s2.get_all()) == {"pk1": "evt1"}


def test_sqlite_namespaces_by_route(tmp_path):
    db = str(tmp_path / "links.db")
    a = SqliteLinkStore(db, route="route-a")
    b = SqliteLinkStore(db, route="route-b")
    asyncio.run(a.put("pk", "a-id"))
    asyncio.run(b.put("pk", "b-id"))
    assert asyncio.run(a.get_all()) == {"pk": "a-id"}   # same key, isolated per route
    assert asyncio.run(b.get_all()) == {"pk": "b-id"}


def test_both_satisfy_the_protocol(tmp_path):
    assert isinstance(InMemoryLinkStore(), LinkStore)
    assert isinstance(SqliteLinkStore(str(tmp_path / "x.db")), LinkStore)


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
