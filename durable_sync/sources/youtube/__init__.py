"""YouTube source: a channel's uploads -> Records.

YouTube exposes no per-video author, so attribution (if you need it) is an
app-side concern: the Record carries a "Scan Text" field and the enrich hook gets
a YouTubeVideoContext for inverted name-matching against your own roster.

Requires the `youtube` extra:  pip install "durable-sync[youtube]"
"""
from __future__ import annotations

from durable_sync.sources.youtube.source import YouTubeConfig, YouTubeSource, YouTubeVideoContext

__all__ = ["YouTubeSource", "YouTubeConfig", "YouTubeVideoContext"]
