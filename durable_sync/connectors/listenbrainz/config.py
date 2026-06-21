"""Config for the ListenBrainz destination. Secrets are read from the env var
NAMED here (never hardcoded), matching the rest of the library."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ListenBrainzConfig:
    """Everything a deployment supplies. Get a user token from
    https://listenbrainz.org/settings/ ; `user_name` is your ListenBrainz handle."""
    user_name: str
    token_env: str = "LISTENBRAINZ_TOKEN"
    # MusicBrainz REQUIRES a descriptive User-Agent and rate-limits anonymous
    # callers to ~1 req/sec — override the contact URL for your deployment.
    user_agent: str = "durable-sync/0.1 ( https://github.com/temporal-community/durable-sync )"
    # Sleep before each MusicBrainz ISRC lookup to stay under the rate limit. Cache
    # hits (LinkStore) skip the lookup entirely, so this only paces genuine misses.
    mb_pacing_seconds: float = 1.1
