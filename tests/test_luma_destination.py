"""Unit tests for LumaDestination: pure event encoding + the link-store-backed
idempotency (Luma can't hold a foreign key). No network."""
from __future__ import annotations

import asyncio
import datetime as dt

from durable_sync.core import Record
from durable_sync.connectors.luma import InMemoryLinkStore, LumaDestination
from durable_sync.connectors.luma.destination import _encode_event

NOW = dt.datetime(2026, 6, 20, tzinfo=dt.timezone.utc)


def _dest(**kw) -> LumaDestination:
    return LumaDestination(link_store=InMemoryLinkStore(), **kw)


def test_encode_minimal_event():
    d = _dest()
    rec = Record(primary_key="n1", properties={"Name": "Launch", "Date": "2026-07-01T17:00:00Z",
                                               "Unmapped": "dropped"})
    payload = _encode_event(d, rec)
    assert payload["name"] == "Launch"
    assert payload["start_at"] == "2026-07-01T17:00:00Z"
    assert payload["timezone"] == "UTC"
    assert "Unmapped" not in payload          # Luma has a fixed schema; extras dropped


def test_update_skips_create_only_date():
    d = _dest(create_only_properties={"Date"})
    rec = Record(primary_key="n1", properties={"Name": "Renamed", "Date": "2026-07-02T00:00:00Z"})
    payload = _encode_event(d, rec, creating=False)
    assert payload["name"] == "Renamed"       # title still refreshes
    assert "start_at" not in payload          # create-only date NOT overwritten on update


def test_is_auth_error_on_401():
    assert LumaDestination.is_auth_error(RuntimeError("Luma POST /event/create -> 401: bad key"))
    assert not LumaDestination.is_auth_error(RuntimeError("Luma POST /event/create -> 500: boom"))


def test_linkstore_roundtrip_is_idempotency_source():
    # query_existing_ids reads the app-owned store; create() writes to it.
    store = InMemoryLinkStore()
    assert asyncio.run(store.get_all()) == {}
    asyncio.run(store.put("n1", "evt-123"))
    assert asyncio.run(store.get_all()) == {"n1": "evt-123"}


def test_create_records_link_via_fake_api(monkeypatch):
    """create() must persist primary_key -> event id, or the next sync duplicates."""
    import durable_sync.connectors.luma.destination as moddest

    async def fake_create_event(client, payload):
        return "evt-new"

    monkeypatch.setattr(moddest.api, "create_event", fake_create_event)

    store = InMemoryLinkStore()
    dest = LumaDestination(link_store=store)
    session = moddest._LumaSession(client=None, dest=dest)
    rec = Record(primary_key="n1", properties={"Name": "X", "Date": "2026-07-01T00:00:00Z"})
    wrote = asyncio.run(session.create(rec, NOW))
    assert wrote is True
    assert asyncio.run(store.get_all()) == {"n1": "evt-new"}   # mapping recorded


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
