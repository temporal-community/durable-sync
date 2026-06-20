"""The fetch_source activity adapts to a Source's paging capability.

A source with `fetch_page` is paginated by the spine; one without it returns the
whole unit as a single page (next_cursor=None). Run offline via ActivityEnvironment.
"""
from __future__ import annotations

import asyncio

from temporalio.testing import ActivityEnvironment

from durable_sync.activities import make_activities, FetchPage
from durable_sync.core import Record, SourceSpec
from tests.memory_destination import MemoryDestination

SPEC = SourceSpec(key="u")


def _fetch_activity(source):
    fetch, _sync = make_activities(source, MemoryDestination())
    return fetch


def _run(source, *args):
    return asyncio.run(ActivityEnvironment().run(_fetch_activity(source), *args))


class WholeListSource:
    name = "whole"

    def specs(self):
        return [SPEC]

    async def fetch(self, spec, only_items=None):
        return [Record(primary_key="a", properties={}), Record(primary_key="b", properties={})]


class PagingSource:
    name = "paging"
    PAGES = {None: (["1", "2"], "c1"), "c1": (["3"], None)}

    def specs(self):
        return [SPEC]

    async def fetch(self, spec, only_items=None):
        raise AssertionError("fetch_page should be preferred over fetch()")

    async def fetch_page(self, spec, only_items, cursor):
        ids, nxt = self.PAGES[cursor]
        return [Record(primary_key=i, properties={}) for i in ids], nxt


def test_non_paging_source_returns_single_page():
    page = _run(WholeListSource(), SPEC, None, None)
    assert isinstance(page, FetchPage)
    assert [r.primary_key for r in page.records] == ["a", "b"]
    assert page.next_cursor is None  # one page, done


def test_paging_source_is_threaded_by_cursor():
    first = _run(PagingSource(), SPEC, None, None)
    assert [r.primary_key for r in first.records] == ["1", "2"]
    assert first.next_cursor == "c1"
    second = _run(PagingSource(), SPEC, None, "c1")
    assert [r.primary_key for r in second.records] == ["3"]
    assert second.next_cursor is None  # last page
