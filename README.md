# durable-sync

**Durable, idempotent source → destination sync, built on [Temporal](https://temporal.io).**

Pull from anything, upsert into anything — crash-safe, retry-safe, scheduled, and
unattended. The hard parts (durable orchestration, idempotent upsert, headless
OAuth refresh, pagination, rate-limit backoff, auth-failure-pauses-don't-hammer)
live in the spine; you implement two small seams. GitHub → Notion is the
reference wiring.

> **Status: reference wiring complete.** The spine (generic activities + entity
> sync workflow + worker/bootstrap), payload codec, the GitHub source, and both
> the Notion (workflow-owned OAuth) and Asana (REST + self-serve PAT)
> destinations are all in, with tests. See what's built below.

## The whole thing in two seams

```
  Source.fetch(spec) ─► [Record, …] ─► Destination upserts (idempotent, keyed on primary_key)
```

- **`Record`** — `{primary_key, properties, body}`. `properties` are *neutral*
  Python values (str/int/bool/list/date/datetime); the **destination** owns all
  wire-encoding, so a source author never learns a destination's quirks.
- **`primary_key`** — the immutable idempotency key (e.g. a repo id), never a
  name/URL. This is what makes Temporal's at-least-once retries safe.
- Implement `Source` for your data, `Destination` for your target. Everything
  else is inherited.

## Why a workflow owns the OAuth token

Most teams can't get an admin-issued API token for their workspace. durable-sync's
Notion destination authorizes as *an individual* over OAuth + dynamic client
registration (no admin), and a long-running Temporal workflow owns the rotating
refresh token — serializing refreshes (no rotation race), surviving restarts, and
serving fresh access tokens via query so the secret never enters event history.
The destination protocol is deliberately neither transport- nor auth-shaped: the
Asana destination uses plain REST + a self-serve token to prove it.

## Install

```bash
pip install "durable-sync[notion]"     # destination extras: notion / asana
pip install "durable-sync[github]"     # source extras: github / luma / youtube / contentful
pip install "durable-sync[crypto]"     # opt-in AES-GCM payload encryption
pip install "durable-sync[all,dev]"    # everything + tests
```

## Layout

```
durable_sync/
├── core.py             Record + Source/Destination protocols (sandbox-safe spine)
├── activities.py       generic fetch_source / sync_records (built by a factory)
├── workflows/sync.py   SourceSyncWorkflow — one durable entity workflow per source unit
├── worker.py           assembles the worker (+ a destination's aux workflows/activities)
├── bootstrap.py        starts one entity workflow per source unit (idempotent)
├── codec.py            opt-in AES-GCM payload codec
├── config.py           runtime/connection config
├── temporal_client.py  client with the codec wired in
├── auth/oauth/         OAuth-as-a-workflow toolkit (token-owner workflow + flow)
├── http.py             shared httpx retry/backoff for REST sources & destinations
├── sources/
│   ├── content.py      shared neutral column vocabulary for content-style sources
│   ├── multi.py        MultiSource — fan several sources onto one worker/bootstrap
│   ├── github/         orgs + named repos -> Records, with an enrichment hook
│   ├── luma/           Luma calendar events (+ host context for the enrich hook)
│   ├── youtube/        a channel's uploads (inverted-match scan text for enrich)
│   └── contentful/     entries by content type (CDA preferred, CMA fallback)
└── destinations/
    ├── notion/         MCP transport + workflow-owned OAuth
    └── asana/          direct REST + self-serve PAT
```

## What's built

- [x] Core spine (`Record`, `Source`/`Destination` protocols)
- [x] Generic activities + entity sync workflow + worker/bootstrap
- [x] Payload encryption codec
- [x] OAuth-as-a-workflow toolkit (token-owner workflow, PKCE + dynamic client registration)
- [x] Notion destination (workflow-owned OAuth, Bearer transport, 429 backoff, pacing)
- [x] Asana destination (REST + self-serve PAT)
- [x] GitHub source (parameterized orgs/repos, with an enrichment hook)
- [x] Luma / YouTube / Contentful sources (events / videos / CMS entries — async httpx, shared backoff)
- [x] `MultiSource` (run several sources on one worker) + shared content-column vocabulary
- [x] Tests (offline spine smoke via `MemoryDestination`, encode + normalizer unit tests, live smokes)

See [CONTRIBUTING.md](CONTRIBUTING.md) to add your own source / destination / auth / transformation.

## License

MIT — see [LICENSE](LICENSE).

---

Merged from two converging Temporal-DevRel projects: a GitHub→Notion demo-catalog
sync and a multi-source DevRel-reporting ingester.
