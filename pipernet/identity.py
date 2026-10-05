"""PiperChat identity layer — client-side ECDSA P-256 keypairs.

"Your identity shouldn't belong to a platform" (deck slide 9): a chat
identity is a WebCrypto P-256 keypair generated *in the browser*; the
private key never leaves the client and no server-side password/name
database ever exists. Every chat message is signed, and a handle is
bound to the first public key that claimed it — typing someone's name
gets you rejected with 403, not accepted.
"""

import hashlib

from .ecdsa import p256_verify

# message envelope: {"name", "pub", "ts", "text", "sig"} — hex fields
MAX_NAME = 32
MAX_TEXT = 2000


def parse_pub(env: dict) -> bytes:
    raw = bytes.fromhex(env["pub"])
    if len(raw) != 65 or raw[0] != 4:
        raise IdentityError("malformed public key")
    return raw


def parse_sig(env: dict) -> bytes:
    s = env["sig"]
    if isinstance(s, bytes):
        sig = s
    else:
        sig = bytes.fromhex(s)
    if not 8 <= len(sig) <= 80:
        raise IdentityError("malformed signature")
    return sig


def signed_bytes(env: dict) -> bytes:
    """The exact byte string the client signed. ts is a plain integer
    millisecond string so JS/Python float formatting can never drift."""
    return f'piperchat/v1\n{env["name"]}\n{env["ts"]}\n{env["text"]}'.encode()


def verify_env(env: dict) -> bool:
    try:
        name, text, ts = str(env["name"]), str(env["text"]), str(env["ts"])
        if not name or len(name) > MAX_NAME or len(text) > MAX_TEXT or not ts.isdigit():
            return False
        return p256_verify(parse_pub(env), signed_bytes(env), parse_sig(env))
    except (KeyError, ValueError):
        return False


class IdentityError(ValueError):
    """Rejected: unsigned, forged, or name-squatting chat traffic."""


def env_id(env: dict) -> str:
    """Content-addressed message id (dedupe across gossip)."""
    sig = env["sig"] if isinstance(env["sig"], bytes) else env["sig"].encode()
    return hashlib.sha256(signed_bytes(env) + sig.lower()).hexdigest()


class IdentityRegistry:
    """Per-node binding of handles to public keys.

    First pubkey to claim a handle owns it, forever. A pubkey may keep
    posting only under its own handle. Name changes require a fresh
    handle that is not owned by anybody.
    """

    def __init__(self):
        self.handle_owner: dict[str, str] = {}   # handle -> pub hex (lower)
        self.pub_handle: dict[str, str] = {}     # pub hex -> handle

    def bind(self, env: dict):
        handle, pub = env["name"], env["pub"].lower()
        owner = self.handle_owner.get(handle)
        if owner is None:
            prev = self.pub_handle.get(pub)
            if prev is not None and prev != handle:
                raise IdentityError(
                    f"key already owns handle '{prev}' — one key, one handle"
                )
            self.handle_owner[handle] = pub
            self.pub_handle[pub] = handle
        elif owner != pub:
            raise IdentityError(f"handle '{handle}' belongs to a different key")

    def owner_of(self, handle: str):
        return self.handle_owner.get(handle)
