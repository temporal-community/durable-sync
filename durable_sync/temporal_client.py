"""One place to open a Temporal client, with the encryption codec applied.

Wiring the codec here (not at each call site) keeps encode/decode consistent
across the worker, starters, and the token accessor — otherwise one un-codec'd
client would read ciphertext it can't decrypt.
"""
from __future__ import annotations

import dataclasses

from temporalio.client import Client
from temporalio.converter import default as default_converter

from durable_sync import config
from durable_sync.codec import encryption_codec


async def connect() -> Client:
    codec = encryption_codec()
    converter = default_converter()
    if codec is not None:
        converter = dataclasses.replace(converter, payload_codec=codec)

    kwargs: dict = {
        "namespace": config.TEMPORAL_NAMESPACE,
        "data_converter": converter,
    }
    if config.TEMPORAL_API_KEY:  # Temporal Cloud: API-key auth over TLS
        kwargs["api_key"] = config.TEMPORAL_API_KEY
        kwargs["tls"] = True
    return await Client.connect(config.TEMPORAL_ADDRESS, **kwargs)
