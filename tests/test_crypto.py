"""RFC-vector tests for pipernet.crypto (X25519, ChaCha20, Poly1305, HKDF)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipernet.crypto import (
    aead_chacha20_poly1305_open,
    aead_chacha20_poly1305_seal,
    chacha20_xor,
    hkdf_sha256,
    poly1305,
    poly1305_key_gen,
    session_keys,
    x25519,
    x25519_secret,
    x25519_shared,
)

hx = bytes.fromhex


# --- X25519 (RFC 7748 sec 5.2 + 6.1) ---------------------------------------

def test_x25519_rfc7748_5_2_vector_1():
    out = x25519(
        hx("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4"),
        hx("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c"),
    )
    assert out.hex() == "c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552"


def test_x25519_rfc7748_5_2_vector_2():
    out = x25519(
        hx("4b66e9d4d1b4673c5ad22691957d6af5c11b6421e0ea01d42ca4169e7918ba0d"),
        hx("e5210f12786811d3f4b7959d0538ae2c31dbe7106fc03c3efc4cd549c715a493"),
    )
    assert out.hex() == "95cbde9476e8907d7aade45cb4b873f88b595a68799fa152e6f8f7647aac7957"


def test_x25519_rfc7748_iterated_once():
    base = (9).to_bytes(32, "little")
    k = hx("0900000000000000000000000000000000000000000000000000000000000000")
    assert x25519(k, base).hex() == "422c8e7a6227d7bca1350b3e2bb7279f7897b87bb6854b783c60e80311ae3079"


def test_x25519_rfc7748_diffie_hellman():
    a = hx("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
    b = hx("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
    pub_a = "8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a"
    pub_b = "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f"
    assert x25519(a, (9).to_bytes(32, "little")).hex() == pub_a
    assert x25519(b, (9).to_bytes(32, "little")).hex() == pub_b
    shared = hx("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742")
    assert x25519(a, x25519(b, (9).to_bytes(32, "little"))) == shared
    assert x25519(b, x25519(a, (9).to_bytes(32, "little"))) == shared


def test_x25519_random_secret_agreement():
    priv_a, pub_a = x25519_secret()
    priv_b, pub_b = x25519_secret()
    k1, k2 = x25519_shared(priv_a, pub_b), x25519_shared(priv_b, pub_a)
    assert k1 == k2 and len(k1) == 32 and k1 != b"\x00" * 32
    keys_a = session_keys(priv_a, pub_b)
    keys_b = session_keys(priv_b, pub_a)
    assert len(keys_a) == 2 and len(keys_b) == 2 and len(keys_a[0]) == 32
    assert keys_a == keys_b or keys_a[0] == keys_b[1]


# --- ChaCha20 (RFC 8439 sec 2.3.2 + 2.4.2) ----------------------------------

def test_chacha20_block_function_vector():
    keystream = chacha20_xor(
        hx("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
        1,
        hx("000000090000004a00000000"),
        b"\x00" * 64,
    )
    assert keystream.hex() == (
        "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4e"
        "d2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e"
    )


def test_chacha20_rfc8439_2_4_2_vector():
    plaintext = (
        "Ladies and Gentlemen of the class of '99: If I could offer you "
        "only one tip for the future, sunscreen would be it."
    ).encode()
    ciphertext = chacha20_xor(
        hx("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
        1,
        hx("000000000000004a00000000"),
        plaintext,
    )
    assert ciphertext.hex() == (
        "6e2e359a2568f98041ba0728dd0d6981e97e7aec1d4360c20a27afccfd9fae0b"
        "f91b65c5524733ab8f593dabcd62b3571639d624e65152ab8f530c359f0861d8"
        "07ca0dbf500d6a6156a38e088a22b65e52bc514d16ccf806818ce91ab7793736"
        "5af90bbf74a35be6b40b8eedf2785e42874d"
    )


# --- Poly1305 (RFC 8439 sec 2.5.2) -------------------------------------------

def test_poly1305_rfc8439_vector():
    tag = poly1305(
        hx("85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b"),
        b"Cryptographic Forum Research Group",
    )
    assert tag.hex() == "a8061dc1305136c6c22b8baf0c0127a9"


def test_poly1305_key_generation_vector():
    otk = poly1305_key_gen(
        hx("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f"),
        hx("000000000001020304050607"),
    )
    assert otk.hex() == ("8ad5a08b905f81cc81504027 4ab29471"
                         "a833b637e3fd0da508dbb8e2fdd1a646").replace(" ", "")


# --- AEAD_CHACHA20_POLY1305 (RFC 8439 sec 2.8.2) ------------------------------

def test_aead_rfc8439_2_8_2_vector():
    plaintext = (
        "Ladies and Gentlemen of the class of '99: If I could offer you "
        "only one tip for the future, sunscreen would be it."
    ).encode()
    sealed = aead_chacha20_poly1305_seal(
        hx("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f"),
        hx("070000004041424344454647"),
        plaintext,
        aad=hx("50515253c0c1c2c3c4c5c6c7"),
    )
    assert sealed.hex() == (
        "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d6"
        "3dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b36"
        "92ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc"
        "3ff4def08e4b7a9de576d26586cec64b6116"
        "1ae10b594f09e26a7e902ecbd0600691"
    )


def test_aead_roundtrip_and_tamper_rejection():
    key = os.urandom(32)
    nonce = os.urandom(12)
    data = os.urandom(1_000) + b"exact multiple" + os.urandom(7)
    for aad in (b"", b"auth-data"):
        sealed = aead_chacha20_poly1305_seal(key, nonce, data, aad=aad)
        assert aead_chacha20_poly1305_open(key, nonce, sealed, aad=aad) == data
        bad = sealed[:-1] + bytes([sealed[-1] ^ 1])
        try:
            aead_chacha20_poly1305_open(key, nonce, bad, aad=aad)
            raise AssertionError("tampered frame accepted")
        except ValueError:
            pass


# --- HKDF-SHA256 (RFC 5869 sec A.1 / A.3) --------------------------------------

def test_hkdf_rfc5869_case_1():
    okm = hkdf_sha256(
        ikm=b"\x0b" * 22,
        salt=hx("000102030405060708090a0b0c"),
        info=hx("f0f1f2f3f4f5f6f7f8f9"),
        length=42,
    )
    assert okm.hex() == ("3cb25f25faacd57a90434f64d0362f2a"
                         "2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
                         "34007208d5b887185865")


def test_hkdf_rfc5869_case_3_empty_salt_info():
    okm = hkdf_sha256(ikm=b"\x0b" * 22, salt=b"", info=b"", length=42)
    assert okm.hex() == ("8da4e775a563c18f715f802a063c5a31"
                         "b8a11f5c5ee1879ec3454e5f3c738d2d"
                         "9d201395faa4b61a96c8")
