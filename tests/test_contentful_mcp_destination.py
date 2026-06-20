"""Unit tests for ContentfulMcpDestination (no network): LinkStore idempotency,
version-on-update, and the tolerant publish gate. Uses a fake ContentfulMcp."""
from __future__ import annotations

import asyncio
import datetime as dt

from durable_sync.core import Record
from durable_sync.linkstore import InMemoryLinkStore
from durable_sync.connectors.contentful import ContentfulMcpDestination
from durable_sync.connectors.contentful.mcp_destination import _McpSession

NOW = dt.datetime(2026, 6, 20, tzinfo=dt.timezone.utc)


class _FakeCf:
    def __init__(self, *, publish_exc=None, version=3):
        self.created, self.updated, self.published = [], [], []
        self._publish_exc, self._version = publish_exc, version

    async def create_entry(self, content_type, fields):
        self.created.append((content_type, fields))
        return "ENTRY1"

    async def entry_version(self, entry_id):
        return self._version

    async def update_entry(self, entry_id, fields, version):
        self.updated.append((entry_id, fields, version))

    async def publish_entry(self, entry_id):
        if self._publish_exc:
            raise self._publish_exc
        self.published.append(entry_id)


def _dest(**kw):
    async def _tp() -> str:
        return "tok"
    return ContentfulMcpDestination(
        space_id="s", content_type="card", field_map={"Name": "title"},
        link_store=InMemoryLinkStore(), token_provider=_tp, **kw)


def _rec():
    return Record(primary_key="notion-1", properties={"Name": "Hello", "Unmapped": "x"})


def test_create_records_link_and_encodes_fields():
    d = _dest()
    s = _McpSession(_FakeCf(), d)
    assert asyncio.run(s.create(_rec(), NOW)) is True
    assert asyncio.run(d.link_store.get_all()) == {"notion-1": "ENTRY1"}     # idempotency link
    ct, fields = s._cf.created[0]
    assert ct == "card"
    assert fields == {"title": {"en-US": "Hello"}}                          # encoded, unmapped dropped


def test_update_reads_then_passes_version():
    d = _dest()
    cf = _FakeCf(version=9)
    s = _McpSession(cf, d)
    asyncio.run(s.update("ENTRY1", _rec(), NOW))
    assert cf.updated == [("ENTRY1", {"title": {"en-US": "Hello"}}, 9)]      # version threaded through


def test_publish_gate_is_tolerated():
    # publish=True but the MCP app forbids publish_entry -> entry still created (draft), no raise.
    err = RuntimeError("You do not have permission to execute the publish_entry tool ...")
    cf = _FakeCf(publish_exc=err)
    s = _McpSession(cf, _dest(publish=True))
    assert asyncio.run(s.create(_rec(), NOW)) is True
    assert cf.published == []                                               # publish skipped, not fatal


def test_publish_happens_when_allowed():
    cf = _FakeCf()
    s = _McpSession(cf, _dest(publish=True))
    asyncio.run(s.create(_rec(), NOW))
    assert cf.published == ["ENTRY1"]


def test_unexpected_publish_error_propagates():
    cf = _FakeCf(publish_exc=RuntimeError("503 upstream boom"))
    s = _McpSession(cf, _dest(publish=True))
    try:
        asyncio.run(s.create(_rec(), NOW))
        assert False, "expected the non-permission error to propagate"
    except RuntimeError as e:
        assert "503" in str(e)


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
