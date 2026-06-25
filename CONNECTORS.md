# Connectors: discovery & where they can live

A connector is **just a class** implementing the `Source` or `Destination`
protocol (`durable_sync/core.py`) plus an **entry point** so apps can find it.
That's the whole plugin model — there is no registry service, no central list to
add yourself to. This guide covers how discovery works and the three places a
connector can live. For the formal, versioned import surface a connector may
depend on, see [`CONTRACT.md`](CONTRACT.md); for protocol signatures and the
hard-won gotchas, see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## The three homes for a connector

Discovery is by **installed distribution**, not by repo membership — so the same
mechanism serves all three:

| Home | Package | When |
|---|---|---|
| **core** | `durable-sync` | Traditional martech/devrel stack (GitHub, Notion, Asana, Contentful, Luma, YouTube, Jira). Maintained + released with the spine. |
| **contrib** | [`durable-sync-contrib`](https://github.com/temporal-community/durable-sync-contrib) | Off-domain / experimental, openly shared (Spotify, ListenBrainz). Same protocol, independent cadence. |
| **your own package** | anything you publish or keep private | Anything else — including a **private, in-house connector** you never intend to share. |

`python -m durable_sync.registry` lists what's installed, grouped by the package
that provides it — so "which home is connector X in?" is answered by "which
distribution, or absent."

## How discovery works

Connectors register through standard `importlib.metadata` entry points (the same
mechanism pytest/flake8 use). Two groups, because a provider is *some sources and
some destinations*:

```toml
[project.entry-points."durable_sync.sources"]
my-source = "my_pkg.source:MySource"

[project.entry-points."durable_sync.destinations"]
my-dest = "my_pkg.destination:MyDestination"
```

Each entry point resolves **directly to a class**. An app resolves it **by
name** and constructs it with its own config:

```python
from durable_sync.registry import load_source, load_destination
from durable_sync.worker import run_worker

source = load_source("my-source")()
destination = load_destination("my-dest")(...your config...)
await run_worker(source, destination)
```

Resolving by name (not import path) is the point: a connector can move between
packages and **no app wiring changes**. `load_*()` imports the connector's deps
lazily, so listing never pulls a connector's `httpx`/`requests`/`mcp` into a
process that only wants names.

## Walkthrough: write & register a connector

1. Implement `Source` and/or `Destination` from `durable_sync.core` (signatures +
   gotchas in `CONTRIBUTING.md` — keep `__init__.py` import-free, delegate
   `is_auth_error` to `auth_error_in_chain`, route HTTP through
   `request_with_retry`, etc.).
2. Declare entry points in your `pyproject.toml` (snippet above).
3. `pip install -e .`, then confirm with `python -m durable_sync.registry` — your
   connector appears under your distribution's name.
4. An app installs your package alongside `durable-sync` and wires you by name.

## Private / in-house connectors

You do **not** need a PR to this repo — or any public release — to use a
connector. Registration is metadata in *your* package; discovery is whatever's
installed in the environment. So a connector to a bespoke internal tool stays
entirely private:

```toml
# acme-durable-connectors/pyproject.toml  (your private repo)
[project]
name = "acme-durable-connectors"
dependencies = ["durable-sync>=0.3"]      # depend on the contract, not the internals

[project.entry-points."durable_sync.sources"]
acme-crm = "acme_connectors.crm.source:AcmeCrmSource"

[project.entry-points."durable_sync.destinations"]
acme-warehouse = "acme_connectors.warehouse.destination:AcmeWarehouseDestination"
```

```python
source = load_source("acme-crm")()
destination = load_destination("acme-warehouse")(...)
```

`python -m durable_sync.registry` then lists `acme-crm` / `acme-warehouse` under
**`acme-durable-connectors`**, right beside core and contrib — a third providing
distribution, nothing more.

What makes this safe and self-contained:

- **No PR, no phone-home.** `durable-sync` has no hardcoded list of "known
  connectors"; `registry.discover()` enumerates whatever distributions in the
  environment advertise into the two entry-point groups. The connector exists
  only where your package is installed.
- **Distribution stays private.** Install from a private index
  (Artifactory / CodeArtifact / Gemfury), a git URL
  (`pip install "git+ssh://git@github.com/acme/acme-durable-connectors"`), or a
  path / monorepo checkout. All register identically. Your CI and workers just
  install `durable-sync` + your package together.
- **You depend only on the contract.** Pin against the
  [`CONTRACT.md`](CONTRACT.md) surface (`core`, `connectors.content`,
  `auth.oauth`, `http`, `transport.mcp`, `schema`, `linkstore`, `registry`),
  versioned by `durable_sync.core.CONTRACT_VERSION`, so a core upgrade won't
  silently break you.
- **Name collisions: last-registered wins.** If two installed packages register
  the same name in a group, the last one loaded shadows the earlier — so you can
  *intentionally* override a core connector by reusing its name
  (`notion`), though a distinct name (`acme-notion`) is clearer. Prefer a
  vendor-prefixed name to avoid surprises.

## See also

- [`CONTRACT.md`](CONTRACT.md) — the versioned import surface + curation policy.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — protocol signatures, transforms, testing,
  and the conventions that will bite you.
