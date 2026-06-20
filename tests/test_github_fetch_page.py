"""GitHubSource.fetch_page paginates an org sweep by GitHub page number.

The reference Source's paged path: the spine threads the cursor (page number) and
each call returns one page + the next cursor (None on the last page). Verified with
a fake httpx client (no network), discovery_mode on so only repos/languages/
contributors endpoints are exercised.
"""
from __future__ import annotations

import asyncio

import httpx

from durable_sync.core import SourceSpec
from durable_sync.connectors.github.source import GitHubSource, GitHubConfig

# Two pages at per_page=2: page 1 -> [r1, r2] (has_more), page 2 -> [r3] (done).
PAGES = {
    1: [{"id": 1, "name": "a", "full_name": "o/a", "html_url": "u1"},
        {"id": 2, "name": "b", "full_name": "o/b", "html_url": "u2"}],
    2: [{"id": 3, "name": "c", "full_name": "o/c", "html_url": "u3"}],
}


class FakeClient:
    def __init__(self):
        self.repo_pages_fetched = []

    async def request(self, method, url, *, headers=None, params=None, json=None):
        req = httpx.Request(method, url)
        if url.endswith("/repos") and "/orgs/" in url:
            page = params["page"]
            self.repo_pages_fetched.append(page)
            return httpx.Response(200, json=PAGES.get(page, []), request=req)
        if url.endswith("/languages"):
            return httpx.Response(200, json={"Python": 100}, request=req)
        if url.endswith("/contributors"):
            return httpx.Response(200, json=[], request=req)
        return httpx.Response(404, json={}, request=req)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _source(monkeypatch):
    # discovery_mode -> no README; no enrich -> no member fetch. per_page=2 to force paging.
    fake = FakeClient()
    monkeypatch.setattr("durable_sync.connectors.github.source.httpx.AsyncClient", lambda **kw: fake)
    cfg = GitHubConfig(sources=[("org", "o")], discovery_mode=True, per_page=2)
    return GitHubSource(cfg), fake


SPEC = SourceSpec(key="org:o", params={"kind": "org", "org": "o"})


def test_fetch_page_threads_cursor_one_page_at_a_time(monkeypatch):
    src, fake = _source(monkeypatch)
    recs1, cur1 = asyncio.run(src.fetch_page(SPEC, None, None))
    assert [r.primary_key for r in recs1] == ["1", "2"]
    assert cur1 == "2"  # more pages
    recs2, cur2 = asyncio.run(src.fetch_page(SPEC, None, cur1))
    assert [r.primary_key for r in recs2] == ["3"]
    assert cur2 is None  # last page
    assert fake.repo_pages_fetched == [1, 2]


def test_fetch_drains_all_pages(monkeypatch):
    src, _fake = _source(monkeypatch)
    recs = asyncio.run(src.fetch(SPEC))
    assert [r.primary_key for r in recs] == ["1", "2", "3"]  # whole sweep, drained
