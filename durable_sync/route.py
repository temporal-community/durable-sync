"""Route — one sync flow: a source -> (transform, field ownership) -> a destination.

A deployment is a *list of routes*. A system's role isn't fixed: Notion is the
source of one route and the destination of another. Two-way sync between A and B
is just two routes (A->B and B->A) — and if you give every field exactly ONE
owning route, no field has two writers, so there's no conflict to resolve:
"two-way sync" decomposes into many one-way *field* flows.

`owns` expresses that: the set of property names this route is allowed to WRITE.
It's compiled into a transform that drops every other property before the
destination sees it. (This is the general form of a destination's
`create_only_properties`, which is the narrower "write-once" case.)

This is the bundling + field-ownership mechanism. Running several routes on ONE
worker (vs. one worker per route) is intentionally NOT here yet — the spine's
activity factory closes over a single (source, destination) pair. For now, one
route = one `run_route(...)` (= one worker); compose more by running more.
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass, replace

from durable_sync.activities import Transform
from durable_sync.core import Destination, Record, Source


def restrict_to_owned(owned: set[str]) -> Transform:
    """A transform that drops any property NOT in `owned`, so a route only WRITES
    the fields it owns. Give every field one owning route and two-way sync has no
    conflicts to resolve. (primary_key is unaffected — it's not a property.)

    Returns a NEW Record (dataclasses.replace) rather than mutating in place: the
    same Record objects flow fetch -> transform -> sync (and are replayed from
    history), so mutating `record.properties` is a surprising side effect for a
    function whose contract is Record -> Record."""
    def _t(record: Record) -> Record:
        return replace(
            record,
            properties={k: v for k, v in record.properties.items() if k in owned},
        )
    return _t


def compose(*transforms: Transform | None) -> Transform | None:
    """Chain transforms left-to-right; a None return short-circuits to drop the
    record. Result is async (awaits any awaitable stage), so it mixes sync + async
    transforms freely — the fetch activity awaits it."""
    stages = [t for t in transforms if t is not None]
    if not stages:
        return None

    async def _t(record: Record) -> Record | None:
        for stage in stages:
            result = stage(record)
            record = await result if inspect.isawaitable(result) else result
            if record is None:
                return None
        return record
    return _t


@dataclass
class Route:
    """A source -> destination flow. `transform` is app logic (rename/derive/
    filter); `owns` restricts which properties this route may write (field
    ownership). The owns-filter runs AFTER the transform, so a transform may
    derive a field the route then writes."""
    source: Source
    destination: Destination
    transform: Transform | None = None
    owns: set[str] | None = None

    def build_transform(self) -> Transform | None:
        own_filter = restrict_to_owned(self.owns) if self.owns is not None else None
        return compose(self.transform, own_filter)


async def run_route(route: Route, *, task_queue: str | None = None) -> None:
    """Run one route's worker (source + destination + the composed transform).
    Bootstrap its entity workflows separately with `start_sources(route.source)`."""
    from durable_sync.worker import run_worker
    await run_worker(
        route.source, route.destination,
        task_queue=task_queue, transform=route.build_transform(),
    )
