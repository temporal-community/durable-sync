"""Generic, vocab-agnostic enrichment hooks shared across consumers.

These are reusable mechanisms (the LLM classifier and its create-only
destination hook); the domain vocabulary, tool schema, and field mapping are
supplied by each app. See `durable_sync.enrich.llm`.
"""
from __future__ import annotations

from durable_sync.enrich.llm import classify, make_llm_session_enrich

__all__ = ["classify", "make_llm_session_enrich"]
