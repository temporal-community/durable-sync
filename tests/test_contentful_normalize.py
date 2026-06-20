"""Pure unit tests for the Contentful entry->Record normalization + the pure
client transforms (locale flatten, author resolution, publish state). No network."""
from __future__ import annotations

from durable_sync.sources.contentful import ContentfulConfig, ContentfulSource
from durable_sync.sources.contentful import api


def _source() -> ContentfulSource:
    return ContentfulSource(ContentfulConfig(
        space_id="space1",
        content_types={"codeExchange": "Code Exchange", "blogPost": "Blog"},
        url_prefixes={"codeExchange": "https://temporal.io/code-exchange/"},
    ))


def _entry(ct_id="codeExchange", published=True, **fields):
    return {
        "sys": {"id": "e1", "createdAt": "2026-04-01T00:00:00Z",
                "contentType": {"sys": {"id": ct_id}}},
        "fields": {"title": "Sample", "slug": "sample", **fields},
        "_published": published,
    }


def test_basic_mapping_with_url_prefix():
    rec = _source()._to_record(_entry(), "Code Exchange",
                               authors=[{"name": "Angie", "email": ""}])
    assert rec.primary_key == "e1"
    p = rec.properties
    assert p["Name"] == "Sample" and p["Type"] == "Code Exchange" and p["Source"] == "Contentful"
    assert p["URL"] == "https://temporal.io/code-exchange/sample"
    assert p["Status"] == "Published"
    assert p["Authors"] == ["Angie"] and p["Author"] == "Angie"


def test_draft_status_and_author_overwrite():
    rec = _source()._to_record(
        _entry(published=False, authorOverwriteText="Community Contributor"),
        "Code Exchange", authors=[{"name": "Angie", "email": ""}],
    )
    assert rec.properties["Status"] == "Draft"
    # Overwrite text wins for the label; resolved names still ride along.
    assert rec.properties["Author"] == "Community Contributor"
    assert rec.properties["Authors"] == ["Angie"]


def test_no_url_prefix_leaves_url_empty():
    rec = _source()._to_record(_entry(ct_id="blogPost"), "Blog", authors=[])
    assert rec.properties["URL"] is None          # no prefix configured for blogPost


def test_has_title():
    from durable_sync.sources.contentful.source import _has_title
    assert _has_title({"fields": {"title": "x"}})
    assert _has_title({"fields": {"name": "x"}})
    assert not _has_title({"fields": {}})


# --- pure client transforms -------------------------------------------------

def test_flatten_entry_picks_default_locale():
    raw = {"sys": {"id": "p1"}, "fields": {"name": {"en-US": "Angie", "fr": "Angèle"}}}
    flat = api._flatten_entry(raw, "en-US")
    assert flat["fields"]["name"] == "Angie"


def test_flatten_entry_falls_back_to_first_locale():
    raw = {"sys": {"id": "p1"}, "fields": {"name": {"fr": "Angèle"}}}
    assert api._flatten_entry(raw, "en-US")["fields"]["name"] == "Angèle"


def test_resolve_authors_array_and_single_link():
    index = {"p1": {"fields": {"name": "Angie"}}, "p2": {"fields": {"name": "Bob"}}}
    entry = {"fields": {"authors": [{"sys": {"id": "p1"}}, {"sys": {"id": "p2"}}]}}
    assert api._resolve_authors(entry, index) == [
        {"name": "Angie", "email": ""}, {"name": "Bob", "email": ""},
    ]
    single = {"fields": {"author": {"sys": {"id": "p1"}}}}
    assert api._resolve_authors(single, index) == [{"name": "Angie", "email": ""}]


def test_is_published():
    assert api._is_published({"sys": {"publishedVersion": 3}})
    assert not api._is_published({"sys": {}})


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ✓ {name}")
    print("\nCONTENTFUL NORMALIZE TESTS PASS ✅")
