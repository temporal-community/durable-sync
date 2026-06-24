"""Generate a destination's schema from a live sample of a source — Layer 3.

The Approach-A front door: instead of hand-authoring a SCHEMA.md, point this at a
wired Source + Destination, it fetches a sample, infers a neutral schema
(`durable_sync.schema.infer_schema`), and asks the destination to materialize it
(`Destination.ensure_schema`). Generic — works for any destination that implements
the optional hook; Notion is the reference. CREATE-ONLY: a destination that's
already configured is left untouched.

    PYTHONPATH=. python -m durable_sync.bootstrap_schema \
        --source myapp.pipeline:SOURCE --destination myapp.pipeline:DESTINATION \
        --name "GitHub Repos" --override State=SELECT

`--source`/`--destination` are dotted `module:OBJECT` paths to the SAME wired
instances the worker uses. Title/key/synced default to the destination's own
`title_property`/`key_property`/`synced_property` when it exposes them, so usually
you only pass `--name` (and any `--override`).

Note: like the worker, the default Notion token provider queries the OAuth workflow,
so the OAuth bootstrap/start steps must already be done and a Temporal server up.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
from typing import Any

from durable_sync.core import Record, Source, SourceSpec
from durable_sync.schema import infer_schema


def _resolve(path: str) -> Any:
    """Resolve a 'pkg.module:OBJECT' dotted path to the live object."""
    module, sep, attr = path.partition(":")
    if not sep or not attr:
        raise SystemExit(f"expected 'module:OBJECT', got {path!r}")
    try:
        return getattr(importlib.import_module(module), attr)
    except (ImportError, AttributeError) as e:
        raise SystemExit(f"could not resolve {path!r}: {e}") from e


def _pick_spec(source: Source, key: str | None) -> SourceSpec:
    specs = source.specs()
    if not specs:
        raise SystemExit(f"source {source.name!r} has no specs to sample")
    if key is None:
        return specs[0]
    for spec in specs:
        if spec.key == key:
            return spec
    raise SystemExit(f"no spec with key {key!r}; have {[s.key for s in specs]}")


async def _sample(source: Source, spec: SourceSpec, n: int) -> list[Record]:
    """Fetch up to `n` records, preferring `fetch_page` so we don't drain a huge
    source just to infer columns."""
    fetch_page = getattr(source, "fetch_page", None)
    if fetch_page is not None:
        out: list[Record] = []
        cursor = None
        while len(out) < n:
            page, cursor = await fetch_page(spec, None, cursor)
            out.extend(page)
            if cursor is None:
                break
        return out[:n]
    return (await source.fetch(spec, None))[:n]


def _overrides(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for pair in pairs:
        name, sep, kind = pair.partition("=")
        if not sep:
            raise SystemExit(f"--override must be NAME=KIND, got {pair!r}")
        out[name] = kind
    return out


async def run(args: argparse.Namespace) -> None:
    source = _resolve(args.source)
    destination = _resolve(args.destination)

    ensure_schema = getattr(destination, "ensure_schema", None)
    if ensure_schema is None:
        raise SystemExit(
            f"destination {getattr(destination, 'name', destination)!r} does not "
            f"implement ensure_schema — schema generation is unsupported for it"
        )

    spec = _pick_spec(source, args.spec)
    records = await _sample(source, spec, args.sample)
    if not records:
        raise SystemExit(f"source returned no records for spec {spec.key!r} to infer from")

    # Title/key/synced default to the destination's own config when present.
    title = args.title or getattr(destination, "title_property", None) or "Name"
    key = args.key or getattr(destination, "key_property", None)
    synced = args.synced or getattr(destination, "synced_property", None)

    schema = infer_schema(
        records, title=title, key=key, synced=synced,
        overrides=_overrides(args.override), name=args.name,
    )

    print(f"sampled {len(records)} record(s) from {source.name!r} spec {spec.key!r}")
    print(f"inferred schema{f' {schema.name!r}' if schema.name else ''}:")
    for col in schema:
        role = "" if col.role.value == "normal" else f"  [{col.role.value}]"
        print(f"    {col.name:<24} {col.kind.value}{role}")

    # Capture the unconfigured hint BEFORE creating, to phrase the next step.
    hint = getattr(destination, "config_hint", "")
    result = await ensure_schema(schema)
    if result is None:
        print("\ndestination already configured — left its schema untouched (create-only).")
        return
    print(f"\ncreated schema -> destination id: {result}")
    if hint:
        print(f"next: configure the destination with this id ({hint}).")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="durable_sync.bootstrap_schema",
                                description="Generate a destination schema from a source sample.")
    p.add_argument("--source", required=True, help="module:OBJECT of the wired Source")
    p.add_argument("--destination", required=True, help="module:OBJECT of the wired Destination")
    p.add_argument("--spec", default=None, help="which spec key to sample (default: first)")
    p.add_argument("--sample", type=int, default=50, help="max records to sample (default: 50)")
    p.add_argument("--name", default=None, help="table/database name to create")
    p.add_argument("--title", default=None, help="title column (default: destination's title_property)")
    p.add_argument("--key", default=None, help="key column (default: destination's key_property)")
    p.add_argument("--synced", default=None, help="synced column (default: destination's synced_property)")
    p.add_argument("--override", action="append", default=[], metavar="NAME=KIND",
                   help="force a column's kind (e.g. State=SELECT); repeatable")
    asyncio.run(run(p.parse_args(argv)))


if __name__ == "__main__":
    main()
