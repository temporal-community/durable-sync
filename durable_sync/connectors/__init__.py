"""Connectors — one subpackage per external system, each exposing the halves it
supports: a `source.py` (read: implements `Source`), a `destination.py` (write:
implements `Destination`), or both, sharing one client + auth.

Grouped by SYSTEM rather than by direction because a system is often both (Notion
is read in one route and written in another), and its read/write sides share a
transport (e.g. Notion's MCP client + OAuth). The neutral `Source`/`Destination`
protocols still live in `durable_sync.core`; this package is only packaging.

Reference systems: github / luma / youtube / contentful (sources today),
notion / asana (destinations today). `content.py` holds the shared neutral column
vocabulary for content-style sources; `multi.py` fans several sources onto one
worker. Import-free on purpose (a connector may contain workflow code).
"""
