"""Contentful connector: entries by content type, BOTH directions.

`ContentfulSource` reads (CDA preferred, CMA fallback — the only mode that sees
drafts); `ContentfulDestination` writes via the CMA (create/update/publish).
Source keep/drop policy for shared types belongs in your enrich/transform hook
(see ContentfulEntryContext). The destination takes a required `LinkStore`
(primary_key -> entry id) since neutral property names don't match content-model
field ids — see the boundary doctrine in CONTRIBUTING.

Requires the `contentful` extra:  pip install "durable-sync[contentful]"
"""
from __future__ import annotations

from durable_sync.connectors.contentful.destination import ContentfulDestination
from durable_sync.connectors.contentful.source import (
    ContentfulConfig,
    ContentfulEntryContext,
    ContentfulSource,
)

__all__ = [
    "ContentfulSource", "ContentfulConfig", "ContentfulEntryContext",
    "ContentfulDestination",
]
