"""Unit tests for ContentfulDestination's pure CMA field encoding. No network
(the CMA HTTP itself is unverified — see the module docstring)."""
from __future__ import annotations

import datetime as dt

from durable_sync.core import Record
from durable_sync.linkstore import InMemoryLinkStore
from durable_sync.connectors.contentful import ContentfulDestination
from durable_sync.connectors.contentful.destination import _encode_fields

NOW = dt.datetime(2026, 6, 20, tzinfo=dt.timezone.utc)


def _dest(**kw) -> ContentfulDestination:
    return ContentfulDestination(
        space_id="space1",
        content_type="blogPost",
        field_map={"Name": "title", "URL": "slug", "Authors": "authorNames", "Seed": "seedField"},
        link_store=InMemoryLinkStore(),
        create_only_properties={"Seed"},
        **kw,
    )


def test_encode_locale_wraps_mapped_fields_and_drops_unmapped():
    rec = Record(primary_key="n1", properties={
        "Name": "Hello", "URL": "hello", "Authors": ["Angie", "Bob"],
        "Unmapped": "x", "Nada": None,
    })
    fields = _encode_fields(_dest(), rec)
    assert fields["title"] == {"en-US": "Hello"}
    assert fields["slug"] == {"en-US": "hello"}
    assert fields["authorNames"] == {"en-US": ["Angie", "Bob"]}   # lists pass through
    assert "Unmapped" not in fields and "Nada" not in fields      # unmapped + None dropped


def test_encode_respects_custom_locale():
    rec = Record(primary_key="n1", properties={"Name": "Hej"})
    fields = _encode_fields(_dest(default_locale="sv-SE"), rec)
    assert fields["title"] == {"sv-SE": "Hej"}


def test_update_skips_create_only():
    rec = Record(primary_key="n1", properties={"Name": "New", "Seed": "changed"})
    create = _encode_fields(_dest(), rec, creating=True)
    update = _encode_fields(_dest(), rec, creating=False)
    assert create["seedField"] == {"en-US": "changed"}    # written on create
    assert "seedField" not in update                      # create-only -> not overwritten
    assert update["title"] == {"en-US": "New"}            # other fields still refresh


def test_configured_requires_space_and_token(monkeypatch):
    d = _dest()
    monkeypatch.delenv("CONTENTFUL_CMA_TOKEN", raising=False)
    assert d.configured is False                          # token missing
    monkeypatch.setenv("CONTENTFUL_CMA_TOKEN", "tok")
    assert d.configured is True


def test_is_auth_error():
    assert ContentfulDestination.is_auth_error(RuntimeError("Contentful PUT entries -> 401: nope"))
    assert not ContentfulDestination.is_auth_error(RuntimeError("422 validation"))


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
