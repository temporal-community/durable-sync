"""Connector discovery via standard entry points — the plugin *story* (issue #10).

A connector is just a class implementing the `Source` / `Destination` protocol
(`core.py`); nothing requires it to live in *this* package. A connector package —
in-repo, or an out-of-repo `durable-sync-contrib` / `durable-sync-yourthing` —
declares entry points so apps resolve it BY NAME instead of import path:

    [project.entry-points."durable_sync.sources"]
    spotify = "durable_sync.connectors.spotify.source:SpotifySource"

    [project.entry-points."durable_sync.destinations"]
    notion = "durable_sync.connectors.notion.destination:NotionDestination"

Resolving by NAME (not import path) is the whole point: it lets a connector move
between packages (core <-> contrib) without changing one line of app wiring —

    from durable_sync.registry import load_source, load_destination
    source = load_source("spotify")()
    destination = load_destination("notion")(data_source_id=..., ...)
    await run_worker(source, destination)

The app still constructs each class with its own config; discovery resolves the
*class*, never the instance. Two groups (not one "connectors" group) because a
provider is some sources and some destinations — Contentful ships two
destinations, Jira/Notion/Luma are both a source AND a destination — so a single
slot per connector wouldn't fit.

This module is import-light and runs OUTSIDE the workflow sandbox (apps, CLIs,
worker/bootstrap startup). Listing never imports a connector; `.load()` pulls a
connector's deps (httpx, requests, mcp, ...) lazily, only when it is requested.
"""
from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points

from durable_sync.core import Destination, Source

SOURCE_GROUP = "durable_sync.sources"
DESTINATION_GROUP = "durable_sync.destinations"


def _eps(group: str) -> dict[str, EntryPoint]:
    # entry_points(group=...) reads metadata only — it does NOT import the target,
    # so this stays cheap and dep-free. On a name clash dict() keeps the last seen,
    # so a contrib package can shadow a core name (documented override behavior).
    return {ep.name: ep for ep in entry_points(group=group)}


def source_names() -> list[str]:
    """Names of every registered source connector (no imports)."""
    return sorted(_eps(SOURCE_GROUP))


def destination_names() -> list[str]:
    """Names of every registered destination connector (no imports)."""
    return sorted(_eps(DESTINATION_GROUP))


def load_source(name: str) -> type[Source]:
    """The Source *class* registered under `name`. Imports the connector's deps
    on call. The app constructs it with its own config: `load_source("x")(...)`."""
    return _load(SOURCE_GROUP, name, "source")


def load_destination(name: str) -> type[Destination]:
    """The Destination *class* registered under `name` (see `load_source`)."""
    return _load(DESTINATION_GROUP, name, "destination")


def _load(group: str, name: str, kind: str):
    eps = _eps(group)
    ep = eps.get(name)
    if ep is None:
        avail = ", ".join(sorted(eps)) or "(none installed)"
        raise LookupError(f"no {kind} connector named {name!r}; available: {avail}")
    return ep.load()


@dataclass(frozen=True)
class ConnectorInfo:
    """One discovered connector, merged across both groups by name — without
    importing it. `distribution` is the package that provides it, which is the
    core / contrib / not-available answer: a name absent here is not installed."""

    name: str
    source: str | None          # "module:Class" or None
    destination: str | None     # "module:Class" or None
    distribution: str           # providing package, e.g. "durable-sync"
    version: str

    @property
    def kinds(self) -> str:
        k = [kind for kind, present in (("source", self.source), ("destination", self.destination)) if present]
        return "+".join(k)


def _dist_label(ep: EntryPoint) -> tuple[str, str]:
    dist = getattr(ep, "dist", None)
    if dist is None:
        return ("(unknown)", "")
    return (dist.name or "(unknown)", dist.version or "")


def discover() -> list[ConnectorInfo]:
    """Every registered connector, grouped by name across both entry-point groups,
    sorted by name. No connector is imported. Drives the listing CLI and lets an
    app/UI answer "which connectors do I have, and from which package?"."""
    merged: dict[str, dict] = {}
    for attr, group in (("source", SOURCE_GROUP), ("destination", DESTINATION_GROUP)):
        for ep in entry_points(group=group):
            dist, version = _dist_label(ep)
            slot = merged.setdefault(
                ep.name,
                {"source": None, "destination": None, "distribution": dist, "version": version},
            )
            slot[attr] = ep.value
            # Prefer a real distribution label if the first group lacked one.
            if slot["distribution"] in ("(unknown)", "") and dist:
                slot["distribution"], slot["version"] = dist, version
    return [
        ConnectorInfo(name=name, **fields)
        for name, fields in sorted(merged.items())
    ]


def _main() -> None:
    infos = discover()
    if not infos:
        print("No connectors registered. Install one (the core extras, or a "
              "durable-sync-contrib package) and re-run.")
        return
    by_dist: dict[tuple[str, str], list[ConnectorInfo]] = {}
    for info in infos:
        by_dist.setdefault((info.distribution, info.version), []).append(info)
    width = max(len(i.name) for i in infos)
    for (dist, version), group in sorted(by_dist.items()):
        header = f"{dist} {version}".strip()
        print(f"\n{header}")
        for info in sorted(group, key=lambda i: i.name):
            print(f"  {info.name:<{width}}  {info.kinds}")
    print()


if __name__ == "__main__":
    _main()
