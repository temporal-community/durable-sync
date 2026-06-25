"""Connector discovery via entry points (durable_sync.registry).

These assert the *mechanism* (names resolve to the right classes, listing groups
by package, unknown names fail loudly) without importing every connector's deps
or hitting the network. They depend on the entry points declared in pyproject;
run after an editable install so the metadata is present.
"""
from __future__ import annotations

import pytest

from durable_sync import registry


def test_known_sources_and_destinations_are_registered():
    sources = set(registry.source_names())
    destinations = set(registry.destination_names())
    # A representative slice of the in-repo connectors (full set asserted via discover()).
    assert {"github", "notion", "jira", "spotify"} <= sources
    assert {"notion", "asana", "jira", "contentful", "contentful-mcp"} <= destinations


def test_load_resolves_the_concrete_class():
    # Resolve the *class*, never an instance — the app supplies config and
    # constructs. Assert structural conformance without instantiating.
    gh = registry.load_source("github")
    assert gh.__name__ == "GitHubSource"
    assert callable(getattr(gh, "specs")) and callable(getattr(gh, "fetch"))

    notion = registry.load_destination("notion")
    assert notion.__name__ == "NotionDestination"
    assert callable(getattr(notion, "connect"))


def test_both_groups_can_share_a_name_for_different_classes():
    # Jira is a source AND a destination — same name, different class per group.
    assert registry.load_source("jira").__name__ == "JiraSource"
    assert registry.load_destination("jira").__name__ == "JiraDestination"


def test_unknown_name_raises_lookup_error_listing_alternatives():
    with pytest.raises(LookupError) as ei:
        registry.load_source("does-not-exist")
    msg = str(ei.value)
    assert "does-not-exist" in msg
    assert "github" in msg  # available names are surfaced to the caller


def test_discover_merges_by_name_and_attributes_a_distribution():
    by_name = {info.name: info for info in registry.discover()}

    contentful = by_name["contentful"]
    assert contentful.source and contentful.destination
    assert contentful.kinds == "source+destination"

    asana = by_name["asana"]
    assert asana.source is None and asana.destination
    assert asana.kinds == "destination"

    # Every in-repo connector is provided by the core distribution today; after
    # extraction the music connectors move to a different `distribution`.
    assert by_name["github"].distribution == "durable-sync"
