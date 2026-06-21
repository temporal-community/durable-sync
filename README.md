# durable-sync

**Keep the tools your team lives in automatically in sync — your events, videos,
repos, and published content flowing into the catalog or tracker you actually use —
without the brittle script that silently dies at 2am.**

Teams end up copy-pasting between tools, or babysitting a homegrown script that
breaks the moment an API hiccups or a token expires. durable-sync is a small Python
library for building syncs that just keep running: pull records from a **source**
(your YouTube channel, your Luma events, a Contentful CMS, a GitHub org) and keep
them continuously, accurately mirrored into a **destination** (a Notion database, an
Asana project). For example —

- every new **YouTube** video shows up as a row in your **Notion** content database,
- your **Luma** events stay mirrored into an **Asana** project,
- your published **Contentful** articles land in a marketing calendar.

You write a little Python to say *where to read* and *where to write*; the library
makes it durable. Built on [Temporal](https://temporal.io). GitHub → Notion is the
reference wiring.

## Why bother (vs. a quick script)

A weekend script works until it doesn't. durable-sync gives you, out of the box:

- **It just stays current.** Each sync runs on its own schedule, forever, keeping
  itself up to date — no cron job to babysit.
- **No duplicates, ever.** Re-runs and retries update the existing row instead of
  creating a second copy (every record carries a stable id).
- **It survives outages.** If your machine restarts or a service goes down
  mid-sync, it resumes exactly where it left off.
- **It waits instead of flailing.** When a login expires or is revoked, the sync
  pauses and tells you — rather than hammering a dead credential.
- **No admin required.** For tools like Notion you can authorize as *yourself* (no
  IT-issued API key), and your login is refreshed safely in the background.
- **It scales.** From 10 records to hundreds of thousands, it pages through them
  without falling over.

(Under the hood that's durable orchestration, idempotent upserts, headless OAuth,
and rate-limit backoff — all inherited from the library, none of it your problem.)

## The mental model: two seams

```
  Source.fetch(spec) ─► [Record, …] ─► Destination upserts (idempotent, keyed on primary_key)
```

- **`Record`** = `{primary_key, properties, body}`. `properties` are *neutral*
  Python values (`str`/`int`/`bool`/`list`/`date`/`datetime`); the **destination**
  owns all wire-encoding, so a source author never learns a destination's quirks.
- **`primary_key`** is the immutable idempotency key (a repo id, an event id) —
  never a name or URL. This is the single most important field: it's what makes
  retries safe.

Everything else — orchestration, OAuth, backoff — lives in the "spine" and is
shared by every connector.

## Requirements

- Python 3.11+
- A Temporal server. For local dev: [`temporal server
  start-dev`](https://docs.temporal.io/cli#start-dev-server) (from the Temporal
  CLI). For production: a self-hosted cluster or [Temporal
  Cloud](https://temporal.io/cloud).

## Quickstart: see it run in two minutes

This runs the whole spine end-to-end with a network-free in-memory destination —
no tokens, no external services.

```bash
pip install "durable-sync[all,dev]"

# In one terminal: a local Temporal dev server
temporal server start-dev

# In another: the offline spine smoke (fetches fake records, upserts them twice,
# proves the second pass updates instead of duplicating)
PYTHONPATH=. python tests/smoke_spine.py
```

You should see a first pass *create* rows and a second pass *update* the same rows
— idempotency in action. Open the Temporal UI (http://localhost:8233) to watch the
workflow.

## Wire your own sync

A source and a destination are just two small classes. Here's a complete,
runnable sketch:

```python
import asyncio
from contextlib import asynccontextmanager

from durable_sync.core import Record, SourceSpec
from durable_sync.worker import run_worker
from durable_sync.bootstrap import start_sources


# 1) A SOURCE: produce neutral Records, keyed on a stable primary_key.
class TasksSource:
    name = "tasks"

    def specs(self):
        # One SourceSpec per independent unit of work — each gets its own workflow.
        return [SourceSpec(key="all", interval_minutes=15)]

    async def fetch(self, spec, only_items=None):
        rows = await my_api.list_tasks()        # however you read your data
        return [
            Record(primary_key=str(r["id"]),    # immutable id — NOT the title
                   properties={"Title": r["title"], "Done": r["completed"]})
            for r in rows
        ]


# 2) A DESTINATION: idempotent upsert. query_existing_ids() decides create vs update.
class PrinterDestination:
    name = "printer"
    configured = True                            # spine refuses to sync if False
    config_hint = "(always configured)"
    create_only_properties = set()               # props written once, never overwritten

    @asynccontextmanager
    async def connect(self):
        yield self                               # this object is also the session

    async def query_existing_ids(self):
        return {}                                # {primary_key: destination_id} already present

    async def create(self, record, synced_at):
        print("CREATE", record.primary_key, record.properties); return True

    async def update(self, existing_id, record, synced_at):
        print("UPDATE", existing_id, record.properties); return True

    @staticmethod
    def is_auth_error(err):
        return False                             # no interactive auth to break


SOURCE, DESTINATION = TasksSource(), PrinterDestination()

async def main():
    await start_sources(SOURCE)                  # ensure one entity workflow per spec (idempotent)
    await run_worker(SOURCE, DESTINATION)        # host the workflow + activities; runs forever

asyncio.run(main())
```

Operate the running sync from the Temporal CLI — the workflow id is
`durable-sync:<spec.key>`:

```bash
# Trigger a sync now instead of waiting for the interval:
temporal workflow signal --workflow-id "durable-sync:all" --name sync_now --input '[]'

# See when it last ran, its stats, and any error:
temporal workflow query  --workflow-id "durable-sync:all" --type status
```

That's the whole contract. For the real interfaces (optional `body`, the
destination session split, source enrichment hooks, paginated `fetch_page`, the
`transform` seam), see [CONTRIBUTING.md](CONTRIBUTING.md).

## Connectors

Reuse a built-in connector instead of writing your own. Each lives in
`durable_sync/connectors/<system>/`:

| System        | Source | Destination | Notes |
|---------------|:------:|:-----------:|-------|
| **GitHub**    |   ✅   |             | Orgs + named repos; per-repo enrichment hook |
| **YouTube**   |   ✅   |             | A channel's uploads |
| **Luma**      |   ✅   |     ✅      | Calendar events (REST); destination needs a `LinkStore` |
| **Contentful**|   ✅   |     ✅      | REST source (CDA/CMA); destination via REST CMA *or* MCP-over-OAuth for SSO-blocked spaces |
| **Spotify**   |   ✅   |             | Liked Songs, keyed on ISRC; workflow-owned OAuth (PKCE, no admin token) |
| **ListenBrainz**|      |     ✅      | "Loved recordings"; resolves ISRC→MBID via MusicBrainz, cached in a `LinkStore` |
| **Notion**    |   ✅   |     ✅      | MCP transport + workflow-owned OAuth (no admin token needed) |
| **Asana**     |        |     ✅      | Direct REST + a self-serve personal token |

A connector is grouped by **system**, not direction, because a system is often both
a source and a destination and the two sides share a client + auth. Under the hood,
a connector composes a **transport** (MCP or REST/`http.py`) with an **auth
mechanism** (workflow-owned OAuth, or an inline token) — the two axes are
independent.

## Key concepts

- **One workflow per source unit.** `Source.specs()` returns a list of
  `SourceSpec`s; each becomes a long-lived [entity
  workflow](https://docs.temporal.io/encyclopedia/temporal-clients#entity-workflow)
  that *is its own timer* (sleeps `interval_minutes`, wakes early on a `sync_now`
  signal) and uses continue-as-new to bound history. No external scheduler.
- **Idempotency is keyed, never inferred.** The upsert does
  `query_existing_ids()` → update-or-create per `primary_key`. Sync only ever
  creates/updates rows it fetched — **it never deletes** — so hand-added data
  survives.
- **OAuth as a workflow.** For services where you can't get an admin token, a
  `OAuthTokenWorkflow` owns the rotating refresh token, serializes refreshes (no
  rotation race), and serves fresh access tokens via query so the secret stays out
  of history. (Pair with the opt-in AES-GCM payload codec to encrypt secrets at
  rest in history too.)
- **`LinkStore` for FK-less destinations.** Some systems (Luma, Contentful over
  MCP) can't store your `primary_key` on their own objects, so the correspondence
  lives in an app-provided durable store. In-memory and SQLite references ship; use
  a real datastore in production.
- **Scales by paging.** Large sources implement `fetch_page` so the spine fetches +
  upserts page-by-page, keeping every payload under Temporal's limits. See the
  Scaling section of [CONTRIBUTING.md](CONTRIBUTING.md).

## Install

```bash
pip install "durable-sync[notion]"     # a destination: notion / asana
pip install "durable-sync[github]"     # a source: github / luma / youtube / contentful / spotify
pip install "durable-sync[crypto]"     # opt-in AES-GCM payload encryption
pip install "durable-sync[all,dev]"    # everything + test deps
```

## Configuration

All runtime config is environment variables (see `durable_sync/config.py`):

| Variable | Purpose |
|----------|---------|
| `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` | Cluster to connect to (defaults to `localhost:7233` / `default`) |
| `TEMPORAL_API_KEY` | Set for Temporal Cloud (enables TLS) |
| `DURABLE_SYNC_TASK_QUEUE` | Task queue name |
| `DURABLE_SYNC_ENC_KEY` | base64 AES-256 key to encrypt payloads in history (`python -m durable_sync.codec` generates one) |
| `DURABLE_SYNC_BUILD_ID` | Opt-in Worker Versioning for safe redeploys of the long-lived workflows |

Connector-specific config (which org, which Notion database, which token env var)
lives in the source/destination you wire up — never in `config.py`.

## Project layout

```
durable_sync/
├── core.py             Record + Source/Destination protocols (the contract)
├── activities.py       generic fetch_source / sync_records
├── workflows/sync.py   SourceSyncWorkflow — one durable entity workflow per source unit
├── worker.py           run_worker(SOURCE, DESTINATION)
├── bootstrap.py        start_sources(SOURCE) — one workflow per spec (idempotent)
├── codec.py            opt-in AES-GCM payload codec
├── auth/oauth/         OAuth-as-a-workflow toolkit (token-owner workflow + flow)
├── transport/mcp.py    generic Model Context Protocol transport (Notion + Contentful)
├── http.py             shared httpx retry/backoff for REST connectors
├── linkstore.py        idempotency map for FK-less destinations
├── route.py            Route = source -> (transform, field ownership) -> destination
└── connectors/         one subpackage per system (github, youtube, luma, contentful, notion, asana)
```

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) is the authoritative guide for adding a source,
destination, auth mechanism, or transformation — with real signatures, the testing
pattern, and the hard-won gotchas (workflow determinism, signal handlers, history
limits, scaling).

## License

MIT — see [LICENSE](LICENSE).
