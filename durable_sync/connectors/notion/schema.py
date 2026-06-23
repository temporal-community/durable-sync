"""Read a Notion data source's *live* schema so consumers can treat Notion as the
source of truth for controlled-vocab options — and survive column renames.

Two robustness wins for LLM-enrichment consumers:
  * **Options as source of truth** — build a strict-tool-use enum from the column's
    current multi-select options instead of a hardcoded list, so editing the vocab
    in the Notion UI needs no code change.
  * **Rename-proof identity** — every Notion column has a stable internal id that
    does NOT change on rename. It's recoverable from any of the column's
    multi-select option URLs (`collectionPropertyOption://<ds>/<PROP_ID>/<OPT>`),
    so a consumer can anchor its field map on the id and resolve the *current* name
    at write time.

Pure parsing (`parse_data_source_state`) is separated from I/O (`fetch_field_specs`)
so the parser is unit-testable without a live Notion session.
"""
from __future__ import annotations

import json
import re
from typing import Any

# The per-column id is the middle path segment of any option URL.
_OPT_ID = re.compile(r"collectionPropertyOption://[^/]+/([^/]+)/")
# notion-fetch embeds the schema as a JSON blob inside this tag.
_STATE = re.compile(r"<data-source-state>\s*(\{.*\})\s*</data-source-state>", re.S)


def parse_data_source_state(result: Any) -> dict[str, dict]:
    """Parse a `notion-fetch` result (raw string, the wrapper dict with a `text`
    field, or an already-parsed data-source-state dict) into:

        { property_name: {"id": str | None, "type": str, "options": [str, ...]} }

    `id` is recovered from an option URL (present for select / multi_select);
    it is None for option-less types (text / number / date), which can't be
    rename-anchored and must be matched by name.
    """
    # The transport may hand us the tool result as a JSON *string* with a "text"
    # field (the inner state JSON escaped inside it) — json-decode it so the inner
    # <data-source-state> un-escapes before we regex it. Also accept an already-
    # parsed dict (claude.ai connector) or a state dict directly.
    obj: Any = result
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except (json.JSONDecodeError, ValueError):
            obj = result  # not JSON -> treat as raw text below
    state: dict = {}
    if isinstance(obj, dict) and "schema" in obj:
        state = obj
    else:
        text = obj.get("text", "") if isinstance(obj, dict) else str(obj)
        m = _STATE.search(text)
        if m:
            try:
                state = json.loads(m.group(1))
            except json.JSONDecodeError:
                state = {}
    schema = state.get("schema") or {}
    out: dict[str, dict] = {}
    for name, spec in schema.items():
        options = spec.get("options") or []
        pid = None
        for opt in options:
            hit = _OPT_ID.search(opt.get("url") or "")
            if hit:
                pid = hit.group(1)
                break
        out[name] = {
            "id": pid,
            "type": spec.get("type"),
            "options": [o["name"] for o in options if o.get("name")],
        }
    return out


async def fetch_field_specs(session, data_source_id: str,
                            field_ids: dict[str, str]) -> dict[str, dict]:
    """Resolve a `{logical_key: stable_property_id}` map against the live schema.

    Returns `{logical_key: {"name": <current name>, "options": [...]}}` for each id
    that still exists — so a rename (name changes, id doesn't) is transparent, and
    options reflect whatever is currently in Notion. Keys whose id is gone are
    omitted (caller decides how to handle a dropped column).
    """
    result = await session.call("notion-fetch", {"id": data_source_id})
    by_name = parse_data_source_state(result)
    by_id = {spec["id"]: (name, spec) for name, spec in by_name.items() if spec["id"]}
    specs: dict[str, dict] = {}
    for key, pid in field_ids.items():
        hit = by_id.get(pid)
        if hit:
            name, spec = hit
            specs[key] = {"name": name, "options": spec["options"]}
    return specs
