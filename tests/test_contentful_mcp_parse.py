"""Unit tests for scraping sys.id / sys.version out of Contentful-MCP's
prose-prefixed pseudo-XML responses (no network). The live shape is confirmed by
smoke_contentful_mcp_write.py; these guard the parser's tricky parts."""
from __future__ import annotations

from durable_sync.connectors.contentful.mcp import entry_id, entry_version_of

# An entity response: the entry's own <sys> has direct id+version; nested
# space/contentType <sys> have their OWN ids that must NOT be mistaken for it.
_ENTRY = """Entry created successfully:
<entry>
  <sys>
    <id>ENTRY123</id>
    <version>3</version>
    <space><sys><type>Link</type><id>0uuz8ydxyd9p</id></sys></space>
    <contentType><sys><id>card</id></sys></contentType>
  </sys>
  <fields><title>x</title></fields>
</entry>"""


def test_scrapes_entity_id_not_nested_space_or_content_type():
    assert entry_id(_ENTRY) == "ENTRY123"
    assert entry_version_of(_ENTRY) == 3


def test_tolerates_prose_prefix_and_missing():
    assert entry_id("Created: <entry><sys><id>abc</id></sys></entry>") == "abc"
    assert entry_id("no xml at all") is None
    assert entry_version_of("<entry><sys><id>a</id></sys></entry>") is None  # no version present


def test_regex_fallback_when_xml_unparseable():
    # Unbalanced/garbled XML -> ET fails -> best-effort regex still finds version.
    assert entry_version_of("oops <sys><version>7</version> <unclosed>") == 7


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
