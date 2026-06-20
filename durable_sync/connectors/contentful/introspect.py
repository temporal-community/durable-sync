"""List a Contentful space's content types + fields, so you can fill in the
destination's content_type / field_map (and the smoke's CONTENTFUL_SMOKE_* vars).

Uses whichever token is set: a Management (CMA) token (api.contentful.com) or a
read-only Delivery (CDA) token (cdn.contentful.com) — both expose the content
model. Prints each content type's id and its title field (Contentful's
`displayField`) in copy-paste-ready form, plus every field and type.

    CONTENTFUL_SPACE_ID=... CONTENTFUL_CMA_TOKEN=... \
        PYTHONPATH=. python -m durable_sync.connectors.contentful.introspect

Requires the `contentful` extra.
"""
from __future__ import annotations

import os
import sys

import httpx

CDA_BASE = "https://cdn.contentful.com"
CMA_BASE = "https://api.contentful.com"


def _describe(field: dict) -> str:
    ftype = field.get("type", "?")
    if ftype == "Array":
        items = field.get("items", {})
        ftype = f"Array<{items.get('linkType') or items.get('type') or '?'}>"
    elif ftype == "Link":
        ftype = f"Link<{field.get('linkType', '?')}>"
    return ftype


def _load_dotenv() -> None:
    """Load a local .env (dev convenience), mirroring config.py. No-op if absent."""
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return
    load_dotenv()


def main() -> None:
    _load_dotenv()
    space = os.environ.get("CONTENTFUL_SPACE_ID")
    env = os.environ.get("CONTENTFUL_ENVIRONMENT", "master")
    cma = os.environ.get("CONTENTFUL_CMA_TOKEN")
    cda = os.environ.get("CONTENTFUL_DELIVERY_TOKEN")
    if not space or not (cma or cda):
        sys.exit("Set CONTENTFUL_SPACE_ID and CONTENTFUL_CMA_TOKEN (or CONTENTFUL_DELIVERY_TOKEN).")

    base, token = (CMA_BASE, cma) if cma else (CDA_BASE, cda)
    url = f"{base}/spaces/{space}/environments/{env}/content_types"
    r = httpx.get(url, headers={"Authorization": f"Bearer {token}"}, params={"limit": 1000}, timeout=30)
    if r.status_code >= 400:
        sys.exit(f"Contentful {r.status_code}: {r.text[:400]}")

    items = r.json().get("items", [])
    print(f"# {len(items)} content type(s) in space {space} (env {env}) via "
          f"{'CMA' if cma else 'CDA'}\n")
    for ct in sorted(items, key=lambda c: c.get("sys", {}).get("id", "")):
        ct_id = ct.get("sys", {}).get("id", "?")
        display = ct.get("displayField") or "?"   # the field used as the entry title
        print(f"## {ct.get('name', '?')}")
        print(f"CONTENTFUL_SMOKE_CONTENT_TYPE={ct_id}")
        print(f"CONTENTFUL_SMOKE_TITLE_FIELD={display}")
        for field in ct.get("fields", []):
            fid = field.get("id", "?")
            mark = "  <- title" if fid == display else ""
            print(f"    {fid} : {_describe(field)}{mark}")
        print()


if __name__ == "__main__":
    main()
