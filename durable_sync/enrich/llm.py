"""Generic LLM enrichment for durable-sync.

A vocab-agnostic Anthropic **strict-tool-use** classifier (`classify`) plus a
NotionDestination `session_enrich` factory (`make_llm_session_enrich`) that runs
it only when a row is first created. Consumers supply the domain pieces:

  * `tool`       — a strict tool schema whose array properties carry `enum`s.
  * `build_meta` — `build_meta(record) -> dict` reading a Record into a flat dict
                   of signals the prompt is rendered from (one key may be `readme`).
  * `field_map`  — tool-output key -> Notion property name.

Everything provider-specific lives here so multiple apps (devrel-demos,
devrel-ships) reuse it; the app keeps only its vocabulary.

The Anthropic SDK is an optional dependency: `pip install "durable-sync[llm]"`.
If it's missing, or `ANTHROPIC_API_KEY` is unset, or the API errors, `classify`
returns `None` and the hook is a no-op — the sync never breaks.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

DEFAULT_MODEL = "claude-haiku-4-5"
_README_CAP = 12_000  # safety cap on README chars handed to the model

EnabledFn = Callable[[], bool]
BuildMeta = Callable[[Any], dict]


def _render(meta: dict[str, Any]) -> str:
    """Flatten a `build_meta()` dict into a prompt: signal lines + the README.

    Vocab-agnostic — every non-`readme` key becomes a `key: value` line (lists
    comma-joined), so a consumer can hand us whatever signals it has.
    """
    readme = str(meta.get("readme") or "")[:_README_CAP]
    lines: list[str] = []
    for key, val in meta.items():
        if key == "readme":
            continue
        if isinstance(val, (list, tuple, set)):
            val = ", ".join(str(v) for v in val if v)
        if val:
            lines.append(f"{key}: {val}")
    signals = "\n".join(lines) or "(none)"
    return f"Repository signals:\n{signals}\n\nREADME:\n{readme}".strip()


def _validate(result: dict[str, Any], tool: dict[str, Any]) -> dict[str, Any]:
    """Coerce the model's tool input to the schema: arrays filtered to their
    `enum` (deduped, order-preserving), strings stripped. Off-vocab labels are
    dropped — Notion has no matching multi-select option for them anyway."""
    props = (tool.get("input_schema") or {}).get("properties") or {}
    out: dict[str, Any] = {}
    for key, spec in props.items():
        val = result.get(key)
        if val is None:
            continue
        if spec.get("type") == "array":
            allowed = (spec.get("items") or {}).get("enum")
            items = [str(v) for v in val if v] if isinstance(val, list) else []
            if allowed is not None:
                allowed_set = set(allowed)
                items = [v for v in items if v in allowed_set]
            seen: set[str] = set()
            out[key] = [v for v in items if not (v in seen or seen.add(v))]
        else:
            out[key] = str(val).strip()
    return out


def classify(tool: dict[str, Any], meta: dict[str, Any], *,
             model: str = DEFAULT_MODEL) -> dict[str, Any] | None:
    """Run one strict-tool-use classification of `meta` against `tool`.

    Returns the validated tool-input dict (arrays clamped to their enums), or
    `None` on any failure (no key / SDK absent / API error / no tool_use block).
    """
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        import anthropic
    except ImportError:
        log.warning("anthropic not installed; skipping LLM enrichment "
                    "(pip install 'durable-sync[llm]')")
        return None
    try:
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model=model or DEFAULT_MODEL,
            max_tokens=1024,
            tools=[tool],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": _render(meta)}],
        )
        block = next((b for b in resp.content if getattr(b, "type", None) == "tool_use"), None)
        if block is None:
            return None
        return _validate(dict(block.input), tool)
    except Exception as e:  # never break the sync on an LLM hiccup
        log.warning("LLM classify failed: %s", e)
        return None


def make_llm_session_enrich(
    *,
    tool: dict[str, Any] | None = None,
    build_meta: BuildMeta,
    field_map: dict[str, str] | None = None,
    model: str = DEFAULT_MODEL,
    enabled: EnabledFn | None = None,
    create_only: bool = True,
    spec_builder=None,
):
    """Build a NotionDestination `session_enrich(session, record, creating)` hook.

    Classifies via the tool and writes results into `record.properties` through a
    `{tool_key: notion_property_name}` map (empty values dropped). Gated to
    row-create when `create_only` (the default); a no-op when `enabled()` is false
    or `classify` returns None. Returns the record unchanged on every skip/failure.

    Two ways to supply the tool + field map:
      * **static** — pass `tool` and `field_map` (vocab fixed in code), or
      * **dynamic** — pass `spec_builder`, an `async (session) -> (tool, field_map)`
        invoked once (cached) with the live MCP session. Use it to source the
        controlled-vocab enums from the destination's *current* schema and to
        resolve rename-safe column ids to their current names (see
        `durable_sync.connectors.notion.schema`).
    """
    cache: dict[str, Any] = {}

    async def _resolve(session):
        if spec_builder is None:
            return tool, field_map
        if "spec" not in cache:
            cache["spec"] = await spec_builder(session)  # (tool, field_map)
        return cache["spec"]

    async def session_enrich(session, record, creating: bool):
        if enabled is not None and not enabled():
            return record
        if create_only and not creating:
            return record
        t, fmap = await _resolve(session)
        if not t or not fmap:
            return record
        result = classify(t, build_meta(record), model=model)
        if result:
            props = record.properties if record.properties is not None else {}
            for key, prop_name in fmap.items():
                value = result.get(key)
                if value:
                    props[prop_name] = value
            record.properties = props
        return record

    return session_enrich
