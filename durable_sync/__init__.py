"""durable-sync: durable, idempotent source -> destination sync on Temporal.

Public API — implement `Source` for your data, `Destination` for your target;
the spine (entity workflow, idempotent upsert, OAuth refresh, backoff) is
inherited. See `connectors/` (one subpackage per system — GitHub/Luma/YouTube/
Contentful sources, Notion/Asana destinations) for reference implementations.
"""
from __future__ import annotations

from durable_sync.core import (
    Destination,
    DestinationSession,
    Record,
    Source,
    SourceSpec,
)

__all__ = [
    "Record",
    "SourceSpec",
    "Source",
    "Destination",
    "DestinationSession",
]

__version__ = "0.1.0"
