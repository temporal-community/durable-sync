"""Pure unit tests for the YouTube video->Record normalization (no network)."""
from __future__ import annotations

from durable_sync.connectors.youtube import YouTubeConfig, YouTubeSource


def _source() -> YouTubeSource:
    return YouTubeSource(YouTubeConfig(channel="@example"))


def test_basic_mapping():
    rec = _source()._to_record({
        "videoId": "abc123",
        "title": "Durable Execution in 5 min",
        "description": "A short demo by Angie.",
        "publishedAt": "2026-05-01T00:00:00Z",
        "viewCount": 4321,
    })
    assert rec.primary_key == "abc123"
    p = rec.properties
    assert p["Name"] == "Durable Execution in 5 min"
    assert p["Type"] == "Video" and p["Source"] == "YouTube"
    assert p["URL"] == "https://www.youtube.com/watch?v=abc123"
    assert p["Status"] == "Published"
    assert p["Author"] == ""                       # YouTube has no per-video author
    assert p["Reach"] == 4321
    # Title + description go to Scan Text for an app's inverted matching.
    assert "Angie" in p["Scan Text"] and "Durable Execution" in p["Scan Text"]


def test_missing_views_and_title():
    rec = _source()._to_record({"videoId": "z", "viewCount": None})
    assert rec.properties["Name"] == "(untitled video)"
    assert rec.properties["Reach"] is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nYOUTUBE NORMALIZE TESTS PASS ✅")
