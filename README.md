# durable-sync

**Durable, idempotent source → destination sync, built on [Temporal](https://temporal.io).**

Pull from anything, upsert into anything — crash-safe, retry-safe, scheduled, and
unattended. The hard parts (durable orchestration, idempotent upsert, headless
OAuth refresh, pagination, rate-limit backoff, auth-failure-pauses-don't-hammer)
live in the spine; you implement two small seams. GitHub → Notion is the
reference wiring.

> **Status: early scaffold.** The spine, payload codec, and the Notion
> destination (workflow-owned OAuth) are in. Generic activities/workflows, the
> GitHub source, the Asana destination, and tests are landing next — see the
> roadmap below.

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
Asana destination (next) uses plain REST + a self-serve token to prove it.

## Install

```bash
pip install "durable-sync[notion]"     # destination extras: notion / asana
pip install "durable-sync[github]"     # source extras
pip install "durable-sync[crypto]"     # opt-in AES-GCM payload encryption
pip install "durable-sync[all,dev]"    # everything + tests
```

## Layout

```
durable_sync/
├── core.py             Record + Source/Destination protocols (sandbox-safe spine)
├── codec.py            opt-in AES-GCM payload codec
├── config.py           runtime/connection config
├── temporal_client.py  client with the codec wired in
├── sources/            where records come from   (github — next)
└── destinations/       where records land
    └── notion/         MCP transport + workflow-owned OAuth
```

## Roadmap

- [x] Core spine (`Record`, `Source`/`Destination` protocols)
- [x] Payload encryption codec
- [x] Notion destination (workflow-owned OAuth, Bearer transport, 429 backoff, pacing)
- [ ] Generic activities + entity sync workflow + worker/bootstrap
- [ ] GitHub source (parameterized, with an enrichment hook)
- [ ] Asana destination (REST + self-serve PAT)
- [ ] Tests (conformance + normalizers)

## License

MIT — see [LICENSE](LICENSE).

---

Merged from two converging Temporal-DevRel projects: a GitHub→Notion demo-catalog
sync and a multi-source DevRel-reporting ingester.
