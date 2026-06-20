"""Encryption codec for Temporal payloads (AES-256-GCM).

When a destination's auth uses a workflow-owned token (e.g. the Notion
auth workflow), the refresh token lives in that workflow's state and in the
refresh activity's input/output — all of which Temporal persists in event
history. This codec encrypts every payload's bytes before they leave the worker,
so secrets are ciphertext at rest in the cluster and the Web UI.

Opt-in: set DURABLE_SYNC_ENC_KEY to a base64-encoded 32-byte key (generate one
with `python -m durable_sync.codec`). With no key set, payloads are unencrypted
(fine for local dev). Wire `encryption_codec()` into your Temporal client's
data_converter so encode/decode stays consistent across the whole system.

Requires the `crypto` extra:  pip install "durable-sync[crypto]"
"""
from __future__ import annotations

import base64
import os
from typing import Iterable

from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec

_ENCODING = b"binary/encrypted"
_KEY_ENV = "DURABLE_SYNC_ENC_KEY"


def load_key() -> bytes | None:
    raw = os.getenv(_KEY_ENV, "")
    if not raw:
        return None
    key = base64.b64decode(raw)
    if len(key) != 32:
        raise ValueError(f"{_KEY_ENV} must decode to 32 bytes (got {len(key)}).")
    return key


class EncryptionCodec(PayloadCodec):
    def __init__(self, key: bytes) -> None:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        self._aesgcm = AESGCM(key)

    async def encode(self, payloads: Iterable[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": _ENCODING},
                data=self._encrypt(p.SerializeToString()),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Iterable[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            if p.metadata.get("encoding") != _ENCODING:
                out.append(p)  # not ours (e.g. written before encryption was on)
                continue
            decrypted = Payload()
            decrypted.ParseFromString(self._decrypt(p.data))
            out.append(decrypted)
        return out

    def _encrypt(self, data: bytes) -> bytes:
        nonce = os.urandom(12)
        return nonce + self._aesgcm.encrypt(nonce, data, None)

    def _decrypt(self, data: bytes) -> bytes:
        return self._aesgcm.decrypt(data[:12], data[12:], None)


def encryption_codec() -> EncryptionCodec | None:
    """The configured codec, or None if DURABLE_SYNC_ENC_KEY is unset (dev mode)."""
    key = load_key()
    return EncryptionCodec(key) if key else None


if __name__ == "__main__":
    # Generate a key to put in your env as DURABLE_SYNC_ENC_KEY=...
    print(base64.b64encode(os.urandom(32)).decode())
