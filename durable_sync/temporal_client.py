"""One place to open a Temporal client, with the encryption codec applied.

Wiring the codec here (not at each call site) keeps encode/decode consistent
across the worker, starters, and the token accessor — otherwise one un-codec'd
client would read ciphertext it can't decrypt.
"""
from __future__ import annotations

import dataclasses
import sys

from temporalio.client import Client
from temporalio.converter import default as default_converter

from durable_sync import config
from durable_sync.codec import encryption_codec


def _is_local_address(addr: str) -> bool:
    host = addr.rsplit(":", 1)[0].strip("[]")
    return host in ("localhost", "127.0.0.1", "::1", "")


async def connect() -> Client:
    codec = encryption_codec()
    converter = default_converter()
    if codec is not None:
        converter = dataclasses.replace(converter, payload_codec=codec)
    elif config.TEMPORAL_API_KEY or not _is_local_address(config.TEMPORAL_ADDRESS):
        # No encryption codec against a non-local cluster: any workflow-owned OAuth
        # refresh/access token is persisted to event history IN CLEARTEXT and is
        # visible in the Web UI. The codec exists precisely to prevent this — set
        # DURABLE_SYNC_ENC_KEY (see `python -m durable_sync.codec`).
        print(
            f"WARNING: connecting to {config.TEMPORAL_ADDRESS} with NO encryption "
            "codec (DURABLE_SYNC_ENC_KEY unset). OAuth tokens will be stored in "
            "Temporal history in cleartext. Set DURABLE_SYNC_ENC_KEY for production.",
            file=sys.stderr,
        )

    kwargs: dict = {
        "namespace": config.TEMPORAL_NAMESPACE,
        "data_converter": converter,
    }
    if config.TEMPORAL_API_KEY:  # Temporal Cloud: API-key auth over TLS
        kwargs["api_key"] = config.TEMPORAL_API_KEY
        kwargs["tls"] = True
    return await Client.connect(config.TEMPORAL_ADDRESS, **kwargs)
