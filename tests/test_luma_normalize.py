"""Pure unit tests for the Luma entry->Record normalization (no network)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from durable_sync.sources.luma import LumaSource

_PAST = "2020-01-01T00:00:00Z"
_FUTURE = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()


def _entry(**event):
    return {"api_id": "evt-1", "event": {"name": "Kickoff", "start_at": _PAST, **event}}


def test_basic_mapping():
    rec = LumaSource()._to_record(
        _entry(url="my-event"),
        hosts=[{"name": "Angie", "email": "a@x.com"}, {"name": "Bob"}],
    )
    assert rec.primary_key == "evt-1"
    p = rec.properties
    assert p["Name"] == "Kickoff"
    assert p["Type"] == "Event" and p["Source"] == "Luma" and p["Source ID"] == "evt-1"
    assert p["URL"] == "https://lu.ma/my-event"     # slug expanded
    assert p["Status"] == "Published"               # start in the past
    assert p["Authors"] == ["Angie", "Bob"]
    assert p["Author"] == "Angie, Bob"


def test_future_event_is_scheduled():
    rec = LumaSource()._to_record(_entry(start_at=_FUTURE), hosts=[])
    assert rec.properties["Status"] == "Scheduled"
    assert rec.properties["Author"] == ""                # no hosts -> neutral (app fills if it wants)
    assert rec.properties["Authors"] == []


def test_absolute_url_passthrough_and_untitled():
    rec = LumaSource()._to_record(
        {"api_id": "evt-2", "event": {"url": "https://lu.ma/abc"}}, hosts=[],
    )
    assert rec.properties["URL"] == "https://lu.ma/abc"
    assert rec.properties["Name"] == "(untitled event)"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nLUMA NORMALIZE TESTS PASS ✅")
