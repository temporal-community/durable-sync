"""Unit tests for scraping sys.id / sys.version out of Contentful-MCP's
prose-prefixed pseudo-XML responses (no network). Fixture is the REAL create_entry
shape from the live smoke — including the things that broke the first attempt:
an invalid `<*>` tag, and space/contentType ids that precede the entry's own id."""
from __future__ import annotations

from durable_sync.connectors.contentful.mcp import entry_id, entry_version_of

# Verbatim-shaped create_entry response (the space id 0uuz8ydxyd9p appears FIRST,
# nested in <space><sys>; the entry id lives in the URN; <*> is not valid XML).
_CREATE = """Entry created successfully:
<newEntry>
  <metadata/>
  <sys>
    <space><sys><type>Link</type><id>0uuz8ydxyd9p</id></sys></space>
    <id>1xX4aii74Sgo88cy2bQRKN</id>
    <type>Entry</type>
    <publishedCounter>0</publishedCounter>
    <version>1</version>
    <fieldStatus><*><en-US>draft</en-US></*></fieldStatus>
    <contentType><sys><id>card</id></sys></contentType>
    <urn>crn:contentful:::content:spaces/0uuz8ydxyd9p/environments/master/entries/1xX4aii74Sgo88cy2bQRKN</urn>
  </sys>
</newEntry>"""


def test_entry_id_from_urn_not_space_or_content_type():
    # The bug we fixed: must NOT return the space id (0uuz8ydxyd9p) or content type (card).
    assert entry_id(_CREATE) == "1xX4aii74Sgo88cy2bQRKN"


def test_version_is_sys_version_not_published_version():
    assert entry_version_of(_CREATE) == 1
    assert entry_version_of("<publishedVersion>5</publishedVersion><version>7</version>") == 7


def test_urnless_fallback_uses_id_after_space_close():
    raw = "<sys><space><sys><id>SPACE</id></sys></space><id>ENTRYID</id></sys>"
    assert entry_id(raw) == "ENTRYID"   # not SPACE


def test_missing_returns_none():
    assert entry_id("no entry here") is None
    assert entry_version_of("no version here") is None


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
