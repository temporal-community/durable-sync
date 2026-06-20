"""Neutral Record -> Contentful CMA `fields` ({field id: {locale: value}}).

Shared by both the REST destination and the MCP destination — the wire shape is
identical (the MCP create_entry/update_entry tools take the same locale-wrapped
fields object), so the encoding lives here once. Pure (no IO).
"""
from __future__ import annotations

import hashlib
from typing import Any

from durable_sync.core import Record


def deterministic_entry_id(primary_key: str) -> str:
    """A stable Contentful entry id derived purely from the source primary_key.

    Contentful lets you CREATE at a client-chosen id (PUT /entries/{id}), and entry
    ids are [A-Za-z0-9_-]{1,64}. A sha256 hex digest (64 chars, charset-safe) fits
    exactly and is collision-free in practice — so the same Record always maps to
    the same entry, which makes create idempotent: a retry after a crashed create
    re-derives the SAME id and updates that entry instead of making a duplicate
    (and an empty/lost LinkStore can no longer cause duplicates either)."""
    return hashlib.sha256(primary_key.encode("utf-8")).hexdigest()  # 64 hex chars


def encode_fields(
    record: Record,
    *,
    field_map: dict[str, str],
    default_locale: str,
    create_only_properties: frozenset[str] | set[str] = frozenset(),
    creating: bool = True,
) -> dict[str, Any]:
    """Map each property through `field_map` (neutral name -> CMA field id),
    locale-wrapping the value. Unmapped properties and Nones are dropped (Contentful
    has a fixed content model); on update, create-only properties are skipped so
    human edits in Contentful survive."""
    out: dict[str, Any] = {}
    for prop, value in record.properties.items():
        if value is None:
            continue
        if not creating and prop in create_only_properties:
            continue
        field_id = field_map.get(prop)
        if not field_id:
            continue
        out[field_id] = {default_locale: value}
    return out
