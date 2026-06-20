"""Contentful source: entries of chosen content types -> Records.

Scoped by content TYPE (Contentful is usually shared across teams). Two auth
modes — a read-only Delivery (CDA) token, preferred; a self-serve Management
(CMA) PAT as fallback (and the only mode that sees drafts). Keep/drop policy for
shared types belongs in your app's enrich/transform hook — ContentfulEntryContext
carries the resolved authors for exactly that.

Requires the `contentful` extra:  pip install "durable-sync[contentful]"
"""
from __future__ import annotations

from durable_sync.connectors.contentful.source import (
    ContentfulConfig,
    ContentfulEntryContext,
    ContentfulSource,
)

__all__ = ["ContentfulSource", "ContentfulConfig", "ContentfulEntryContext"]
