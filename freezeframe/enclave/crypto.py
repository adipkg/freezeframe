"""Transport encryption between banks and the enclave (libsodium via PyNaCl).

- Banks seal batches to the enclave's public key (SealedBox): only the enclave can open them.
- The enclave seals each response to the requesting member's public key: only that member can
  read its own decisions.
In production the enclave public key is bound to a Secure Execution attestation report, so a
bank only sends data to a measured enclave image. Here it is published by the service directly.
"""
import json

from nacl.public import PrivateKey, PublicKey, SealedBox
from nacl.encoding import Base64Encoder


class KeyPair:
    def __init__(self, sk: PrivateKey | None = None):
        self.sk = sk or PrivateKey.generate()

    @property
    def public_b64(self) -> str:
        return self.sk.public_key.encode(Base64Encoder).decode()

    def open(self, token: str) -> dict:
        return json.loads(SealedBox(self.sk).decrypt(token.encode(), encoder=Base64Encoder))


def seal(obj, public_b64: str) -> str:
    pk = PublicKey(public_b64.encode(), encoder=Base64Encoder)
    return SealedBox(pk).encrypt(json.dumps(obj, default=str).encode(), encoder=Base64Encoder).decode()
