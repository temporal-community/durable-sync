"""ListenBrainz destination tests: pure MusicBrainz/feedback parsing, and the
upsert logic against stubbed APIs + an InMemoryLinkStore (no network)."""
from __future__ import annotations

import asyncio
import datetime as dt

from durable_sync.core import Record
from durable_sync.linkstore import InMemoryLinkStore
from durable_sync.connectors.listenbrainz import api
from durable_sync.connectors.listenbrainz.config import ListenBrainzConfig
from durable_sync.connectors.listenbrainz.destination import ListenBrainzDestination

NOW = dt.datetime(2026, 6, 21, 12, 0, tzinfo=dt.timezone.utc)


# --- pure parse helpers -----------------------------------------------------

def test_first_recording_mbid_picks_first():
    payload = {"recordings": [{"id": "mbid-1"}, {"id": "mbid-2"}]}
    assert api.first_recording_mbid(payload) == "mbid-1"


def test_first_recording_mbid_empty_is_none():
    assert api.first_recording_mbid({"recordings": []}) is None
    assert api.first_recording_mbid({}) is None


def test_feedback_payload_shape():
    assert api.feedback_payload("mbid-1") == {"recording_mbid": "mbid-1", "score": 1}
    assert api.feedback_payload("mbid-1", score=0) == {"recording_mbid": "mbid-1", "score": 0}


# --- upsert logic against stubs --------------------------------------------

def _dest(link_store):
    # No MB pacing in tests, and the network is stubbed at the api module.
    return ListenBrainzDestination(
        ListenBrainzConfig(user_name="me", mb_pacing_seconds=0), link_store=link_store
    )


class _Stub:
    """Records calls to the patched api functions."""
    def __init__(self, isrc_map, loved=()):
        self.isrc_map = isrc_map          # isrc -> mbid (or None)
        self.loved = set(loved)
        self.submitted: list[str] = []

    async def resolve(self, client, isrc, *, user_agent):
        return self.isrc_map.get(isrc)

    async def submit(self, client, token, mbid, *, score=1):
        self.submitted.append(mbid)

    async def get_loved(self, client, token, user_name):
        return set(self.loved)


def _patch(monkeypatch, stub):
    monkeypatch.setattr(api, "resolve_isrc_to_mbid", stub.resolve)
    monkeypatch.setattr(api, "submit_feedback", stub.submit)
    monkeypatch.setattr(api, "get_loved_mbids", stub.get_loved)


def test_create_resolves_submits_and_links(monkeypatch):
    stub = _Stub({"ISRC-A": "mbid-a"})
    _patch(monkeypatch, stub)
    store = InMemoryLinkStore()
    dest = _dest(store)

    async def run():
        async with dest.connect() as s:
            wrote = await s.create(Record(primary_key="ISRC-A", properties={"Name": "Song A"}), NOW)
            assert wrote is True
            assert stub.submitted == ["mbid-a"]
            assert await store.get_all() == {"ISRC-A": "mbid-a"}   # cached for next time
            # query_existing_ids reflects the link store
            assert await s.query_existing_ids() == {"ISRC-A": "mbid-a"}

    asyncio.run(run())


def test_unresolvable_isrc_is_skipped(monkeypatch):
    stub = _Stub({"ISRC-A": None})           # MusicBrainz knows nothing
    _patch(monkeypatch, stub)
    store = InMemoryLinkStore()

    async def run():
        async with _dest(store).connect() as s:
            wrote = await s.create(Record(primary_key="ISRC-A", properties={"Name": "?"}), NOW)
            assert wrote is False                # skipped, not created
            assert stub.submitted == []
            assert await store.get_all() == {}   # nothing linked

    asyncio.run(run())


def test_already_loved_skips_redundant_submit_but_still_links(monkeypatch):
    stub = _Stub({"ISRC-A": "mbid-a"}, loved={"mbid-a"})  # user already loved it
    _patch(monkeypatch, stub)
    store = InMemoryLinkStore()

    async def run():
        async with _dest(store).connect() as s:
            wrote = await s.create(Record(primary_key="ISRC-A", properties={"Name": "A"}), NOW)
            assert wrote is True
            assert stub.submitted == []                       # no redundant POST
            assert await store.get_all() == {"ISRC-A": "mbid-a"}

    asyncio.run(run())


def test_update_is_noop(monkeypatch):
    stub = _Stub({})
    _patch(monkeypatch, stub)

    async def run():
        async with _dest(InMemoryLinkStore()).connect() as s:
            assert await s.update("mbid-a", Record(primary_key="ISRC-A", properties={}), NOW) is False
            assert stub.submitted == []

    asyncio.run(run())


if __name__ == "__main__":
    import inspect
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            if inspect.signature(fn).parameters:
                print(f"  - {name} (needs pytest fixtures; run via pytest)")
                continue
            fn()
            print(f"  ✓ {name}")
    print("\nLISTENBRAINZ TESTS PASS ✅ (run `pytest` for the fixture-based ones)")
