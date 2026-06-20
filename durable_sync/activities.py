"""Generic, source/destination-agnostic activities.

A library can't hardcode `from pipeline import SOURCE, DESTINATION` the way a
single app would, so the activities are produced by a FACTORY the app calls once
with its wired Source + Destination:

    worker = Worker(..., activities=make_activities(SOURCE, DESTINATION))

The activities are registered under stable string names (FETCH_SOURCE /
SYNC_RECORDS); the workflow refers to them by those names, so it never imports
these closures and stays sandbox-clean.
"""
from __future__ import annotations

import datetime as dt
import inspect
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Union

from temporalio import activity
from temporalio.exceptions import ApplicationError

from durable_sync.core import Destination, Record, Source, SourceSpec

# Stable activity names — the workflow executes by name (see workflows/sync.py).
FETCH_SOURCE = "fetch_source"
SYNC_RECORDS = "sync_records"


@dataclass
class FetchPage:
    """One page of fetched+transformed records, plus the cursor for the NEXT page
    (None on the last page). A Source that implements `fetch_page` lets the spine
    bound Temporal history for arbitrarily large sources — neither the fetch result
    nor the upsert payload is ever the whole dataset. A Source without it returns
    everything as a single page (next_cursor=None), exactly as before."""
    records: list[Record] = field(default_factory=list)
    next_cursor: str | None = None

# The GENERIC transform seam: Record -> Record (mutate/derive/rename) or None
# (drop it — so transform doubles as a filter). Source- and destination-agnostic;
# may be sync or async. For transforms that need source internals use the source's
# enrich hook; for ones that read the destination use its session_enrich.
Transform = Callable[[Record], Union[Record, None, Awaitable[Union[Record, None]]]]


def make_activities(
    source: Source, destination: Destination, *, transform: Transform | None = None
) -> list:
    """Build the two generic activities, closed over the app's Source +
    Destination (+ optional generic transform). Returns a list ready to hand to a
    Temporal Worker."""

    async def _apply_transform(records: list[Record]) -> list[Record]:
        if transform is None:
            return records
        out: list[Record] = []
        for rec in records:
            res = transform(rec)
            if inspect.isawaitable(res):
                res = await res
            if res is not None:
                out.append(res)
        return out

    @activity.defn(name=FETCH_SOURCE)
    async def fetch_source(
        spec: SourceSpec, only_items: list[str] | None = None, cursor: str | None = None
    ) -> FetchPage:
        """Fetch ONE page of a source unit (optionally just specific items), apply
        the generic transform (which may drop records by returning None), and return
        the records + the next-page cursor.

        Adapts to the Source: if it implements `fetch_page(spec, only_items, cursor)
        -> (records, next_cursor)` the spine paginates (bounded history for big
        sources); otherwise it calls `fetch()` once and returns everything as a
        single page (next_cursor=None) — unchanged behavior."""
        fetch_page = getattr(source, "fetch_page", None)
        if callable(fetch_page):
            records, next_cursor = await fetch_page(spec, only_items, cursor)
        else:
            records = await source.fetch(spec, only_items)
            next_cursor = None
        return FetchPage(records=await _apply_transform(records), next_cursor=next_cursor)

    @activity.defn(name=SYNC_RECORDS)
    async def sync_records(records: list[Record]) -> dict:
        """Idempotent upsert into the Destination, keyed on each primary_key."""
        if not destination.configured:
            raise ApplicationError(
                f"Destination is not configured ({destination.config_hint})",
                type="ConfigError", non_retryable=True,
            )

        synced_at = dt.datetime.now(dt.timezone.utc)
        created = updated = skipped = 0

        # Guard the idempotency key BEFORE any write (counts fold into `skipped`
        # so created+updated+skipped == total still holds):
        #  * a falsy primary_key can't be idempotent — every keyless record
        #    collides on the same empty key (the first creates a row, the rest
        #    "update" that one row, or all overwrite one link). Drop them.
        #  * a duplicate primary_key WITHIN one batch would double-create:
        #    `existing` is queried once up front, so a 2nd occurrence is still
        #    "not existing" and creates a second row. Collapse to the LAST
        #    occurrence (freshest data) so the upsert stays idempotent.
        deduped: dict[str, Record] = {}
        for rec in records:
            if not rec.primary_key:
                skipped += 1
                activity.logger.warning(
                    "Skipping record with empty primary_key (not idempotent): %r",
                    rec.properties,
                )
                continue
            deduped[rec.primary_key] = rec
        to_sync = list(deduped.values())
        in_batch_dupes = (len(records) - skipped) - len(to_sync)
        if in_batch_dupes:
            skipped += in_batch_dupes
            activity.logger.warning(
                "Collapsed %d in-batch duplicate primary_key(s) (last-wins)", in_batch_dupes
            )

        try:
            async with destination.connect() as session:
                existing = await session.query_existing_ids()  # primary_key -> dest id
                for rec in to_sync:
                    existing_id = existing.get(rec.primary_key)
                    if existing_id:
                        wrote = await session.update(existing_id, rec, synced_at)
                        updated += 1 if wrote else 0
                    else:
                        wrote = await session.create(rec, synced_at)
                        created += 1 if wrote else 0
                    skipped += 0 if wrote else 1   # dropped by a destination-side filter
                    activity.heartbeat(rec.primary_key)
        except ApplicationError:
            raise
        except Exception as e:
            # Auth failures are NOT retryable — only a human re-auth fixes them,
            # so the workflow can pause instead of hammering a dead credential.
            # Everything else stays retryable (transient).
            if destination.is_auth_error(e):
                raise ApplicationError(
                    "Destination authorization is no longer valid (token refresh "
                    "failed or was revoked). Re-authorize, then send `resume`.",
                    type="AuthError", non_retryable=True,
                ) from e
            raise

        stats = {"total": len(records), "created": created, "updated": updated, "skipped": skipped}
        activity.logger.info("Sync complete: %s", stats)
        return stats

    return [fetch_source, sync_records]
