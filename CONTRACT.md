# The connector contract

A connector is **just a class** implementing the `Source` or `Destination`
protocol in `durable_sync/core.py`, plus an entry point so apps can find it.
Nothing requires a connector to live in *this* repo — an out-of-repo pip package
can implement the same protocol and an app can wire it. This file is the promise
that makes that safe: what you may import, how we version it, and which
connectors live where.

> Status: introduced in `CONTRACT_VERSION = 1` (see `durable_sync.core`).

## Curation: core / contrib / not available

Every connector is in exactly one of three buckets — this is the whole
discoverability model:

- **core** — shipped in this repo (`durable-sync`). The traditional martech /
  devrel stack: GitHub, Notion, Asana, Contentful, Luma, YouTube, Jira.
  Integration-tested together, released with the spine.
- **contrib** — shipped in `durable-sync-contrib` (a separate repo/package).
  Off-domain or experimental connectors that would dilute the core narrative —
  Spotify, ListenBrainz, future Apple Music. Same protocol, independent release
  cadence, depends on `durable-sync`.
- **not available** — anyone can publish `durable-sync-<yourthing>`; it Just
  Works via entry points without a PR to this repo.

`python -m durable_sync.registry` lists what's installed, grouped by the package
that provides it — so "which bucket is connector X in?" is answered by "which
distribution, or absent."

## Discovery: entry points (not a registry framework)

Connectors are discovered through standard `importlib.metadata` entry points —
the same mechanism pytest/flake8 use. Two groups, because a provider is *some
sources and some destinations* (Jira/Notion/Luma are both; Contentful ships two
destinations), so one slot per "connector" wouldn't fit:

```toml
[project.entry-points."durable_sync.sources"]
spotify = "durable_sync.connectors.spotify.source:SpotifySource"

[project.entry-points."durable_sync.destinations"]
notion = "durable_sync.connectors.notion.destination:NotionDestination"
```

Each entry point resolves **directly to a class**. An app resolves it **by
name**, then constructs it with its own config:

```python
from durable_sync.registry import load_source, load_destination
from durable_sync.worker import run_worker

source = load_source("spotify")()
destination = load_destination("notion")(data_source_id=..., title_property=..., ...)
await run_worker(source, destination)
```

Resolving by name (not import path) is the point: a connector can move between
`durable-sync` and `durable-sync-contrib` and **no app wiring changes** — only
the entry point's location does. Discovery resolves the *class*; the app always
owns config and construction. `load_*()` imports the connector's deps lazily, so
listing never pulls `httpx`/`requests`/`mcp` into a process that only wants names.

## The public import surface (what a contrib connector may depend on)

These modules are the versioned contract. Depend on them from an out-of-repo
connector; we changelog any breaking change and bump `CONTRACT_VERSION`.

| Module | What it gives a connector |
|---|---|
| `durable_sync.core` | `Record`, `SourceSpec`, the `Source` / `Destination` / `DestinationSession` protocols, `auth_error_in_chain`, `DestinationHTTPError`, `CONTRACT_VERSION` |
| `durable_sync.connectors.content` | shared neutral column vocabulary (`content_record`, `P_*`, cursor pack/unpack) for content-style sources |
| `durable_sync.auth.oauth` | the workflow-owned OAuth toolkit (`flow`, `workflow`, `store`, `token`) — reuse it instead of hand-rolling refresh |
| `durable_sync.http` | `request_with_retry` (Retry-After / 429 / rate-limited-403 backoff) for REST connectors |
| `durable_sync.transport.mcp` | generic MCP session/call/tool-listing over streamable-HTTP |
| `durable_sync.schema` | `infer_schema` + the neutral `Schema` types for the optional `ensure_schema` hook |
| `durable_sync.registry` | discovery (`load_source`, `load_destination`, `discover`) |

Anything **not** in this table (the spine internals — `activities`, `workflows`,
`worker`, `bootstrap`, `temporal_client`, `codec`, `config`) is not part of the
contract and may change without a version bump. Connectors are wired *into* those
by the app; they don't import them.

## Versioning

- `durable_sync.core.CONTRACT_VERSION` is an integer, bumped on any
  **backward-incompatible** change to the surface above (a removed/renamed symbol,
  a changed protocol signature, a changed `Record`/`SourceSpec` shape).
- A contrib package pins a floor, e.g. `durable-sync>=0.3` and checks
  `CONTRACT_VERSION >= N` if it needs a specific feature.
- Backward-*compatible* additions (a new optional protocol hook, a new helper)
  do **not** bump it.
- Breaking changes are recorded in `CHANGELOG.md` under the release that makes
  them, with the migration note.

This extends the replay-safety discipline the spine already practices
(`workflow.patched`, `DURABLE_SYNC_BUILD_ID`) to the connector surface: in-flight
workflows and out-of-repo connectors both need a stable thing to stand on.

## Writing a connector in your own repo

1. Implement `Source` and/or `Destination` from `durable_sync.core` (see
   `CONTRIBUTING.md` for signatures and the hard-won gotchas — keep
   `__init__.py` import-free, delegate `is_auth_error` to `auth_error_in_chain`,
   route HTTP through `request_with_retry`, etc.).
2. Declare entry points in your `pyproject.toml` under `durable_sync.sources` /
   `durable_sync.destinations`.
3. `pip install -e .` and confirm `python -m durable_sync.registry` lists you
   under your distribution.
4. An app installs your package alongside `durable-sync` and wires you by name.
