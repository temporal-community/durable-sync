"""LumaSource.fetch_page paginates via Luma's pagination_cursor, carried in the
spine cursor alongside a FROZEN window start (so every page queries one window).

Verified with a fake httpx client (no network).
"""
from __future__ import annotations

import asyncio

import httpx

from durable_sync.connectors import content
from durable_sync.core import SourceSpec
from durable_sync.connectors.luma.source import LumaSource, LumaConfig

PAGES = {
    None: {"entries": [{"api_id": "e1"}, {"api_id": "e2"}], "has_more": True, "next_cursor": "C2"},
    "C2": {"entries": [{"api_id": "e3"}], "has_more": False, "next_cursor": None},
}


class FakeClient:
    def __init__(self):
        self.afters_seen = []

    async def request(self, method, url, *, headers=None, params=None, json=None):
        req = httpx.Request(method, url)
        if url.endswith("/calendar/list-events"):
            self.afters_seen.append(params.get("after"))
            return httpx.Response(200, json=PAGES[params.get("pagination_cursor")], request=req)
        if url.endswith("/event/get"):
            return httpx.Response(200, json={"hosts": []}, request=req)
        return httpx.Response(404, json={}, request=req)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _source(monkeypatch):
    fake = FakeClient()
    monkeypatch.setenv("LUMA_API_KEY", "k")
    monkeypatch.setattr("durable_sync.connectors.luma.source.httpx.AsyncClient", lambda **kw: fake)
    return LumaSource(LumaConfig()), fake


SPEC = SourceSpec(key="events")


def test_fetch_page_threads_cursor_and_freezes_window(monkeypatch):
    src, fake = _source(monkeypatch)
    recs1, cur1 = asyncio.run(src.fetch_page(SPEC, None, None))
    assert [r.primary_key for r in recs1] == ["e1", "e2"]
    assert content.unpack_cursor(cur1)["token"] == "C2"

    recs2, cur2 = asyncio.run(src.fetch_page(SPEC, None, cur1))
    assert [r.primary_key for r in recs2] == ["e3"]
    assert cur2 is None  # last page

    # The window start is frozen across pages (page 2 reuses page 1's `after`).
    assert fake.afters_seen[0] == fake.afters_seen[1]


def test_fetch_drains_all_pages(monkeypatch):
    src, _fake = _source(monkeypatch)
    recs = asyncio.run(src.fetch(SPEC))
    assert [r.primary_key for r in recs] == ["e1", "e2", "e3"]
