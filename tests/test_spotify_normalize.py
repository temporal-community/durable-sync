"""Unit tests for the Spotify source: pure saved-track -> Record normalization,
and the spine-facing fetch_page/fetch contract (offset cursor + ISRC drop), all
with the network stubbed."""
from __future__ import annotations

import asyncio

import pytest

from durable_sync.connectors.spotify import api
from durable_sync.connectors.spotify.source import SpotifySource, SpotifyConfig


def _item(**track):
    base = {
        "id": "track-1",
        "name": "Mr. Blue Sky",
        "external_ids": {"isrc": "GBARL7800123"},
        "external_urls": {"spotify": "https://open.spotify.com/track/abc"},
        "artists": [{"name": "ELO"}, {"name": "Jeff Lynne"}],
        "album": {"name": "Out of the Blue"},
    }
    base.update(track)
    return {"added_at": "2024-03-01T12:00:00Z", "track": base}


def test_basic_mapping_keys_on_isrc():
    rec = SpotifySource()._to_record(_item())
    assert rec is not None
    assert rec.primary_key == "GBARL7800123"      # ISRC, not the Spotify id
    p = rec.properties
    assert p["Name"] == "Mr. Blue Sky"
    assert p["Type"] == "Track" and p["Source"] == "Spotify"
    assert p["Source ID"] == "GBARL7800123"        # content_record mirrors pk here
    assert p["URL"] == "https://open.spotify.com/track/abc"
    assert p["Date"] == "2024-03-01T12:00:00Z"
    assert p["Authors"] == ["ELO", "Jeff Lynne"]
    assert p["Author"] == "ELO, Jeff Lynne"
    assert p["Album"] == "Out of the Blue"
    assert p["Spotify ID"] == "track-1"


def test_missing_isrc_is_dropped():
    assert SpotifySource()._to_record(_item(external_ids={})) is None
    assert SpotifySource()._to_record(_item(external_ids={"isrc": ""})) is None


def test_untitled_and_no_artists():
    rec = SpotifySource()._to_record(
        {"added_at": "2024-01-01T00:00:00Z",
         "track": {"id": "t2", "external_ids": {"isrc": "X"}, "artists": []}}
    )
    assert rec is not None
    assert rec.properties["Name"] == "(untitled track)"
    assert rec.properties["Author"] == ""
    assert rec.properties["Authors"] == []


def _src():
    # Inject a fake token provider so no OAuthTokenWorkflow / network is touched.
    return SpotifySource(SpotifyConfig(page_limit=2), token_provider=_fake_token)


async def _fake_token() -> str:
    return "fake-token"


def test_fetch_page_threads_offset_cursor_and_drops_no_isrc(monkeypatch):
    # Page 1: two items (one with no ISRC -> dropped), more pages remain.
    # Page 2: one item, last page (next is None).
    pages = {
        0: ([_item(), _item(id="t-noisrc", external_ids={})], 2),
        2: ([_item(id="t3", external_ids={"isrc": "Z3"})], None),
    }

    async def fake_page(client, token, *, offset, limit):
        assert token == "fake-token" and limit == 2
        return pages[offset]

    monkeypatch.setattr(api, "list_saved_tracks_page", fake_page)
    src = _src()

    async def run():
        spec = src.specs()[0]
        page1, cur1 = await src.fetch_page(spec, None, None)
        assert [r.primary_key for r in page1] == ["GBARL7800123"]  # noisrc dropped
        assert cur1 is not None
        page2, cur2 = await src.fetch_page(spec, None, cur1)       # cursor threaded
        assert [r.primary_key for r in page2] == ["Z3"]
        assert cur2 is None                                        # last page
        # fetch() drains both pages into one list.
        allrecs = await src.fetch(spec)
        assert [r.primary_key for r in allrecs] == ["GBARL7800123", "Z3"]

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
    print("\nSPOTIFY NORMALIZE TESTS PASS ✅ (run `pytest` for the fixture-based ones)")
