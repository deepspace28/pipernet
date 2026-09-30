"""Reed-Solomon erasure coding over GF(2^8) — stdlib only.

A chunk is split into ``data`` equal-size data shards plus ``parity``
check shards.  ANY ``data`` of the ``data+parity`` shards reconstructs
the original chunk exactly; storage overhead is ``(data+parity)/data``
(1.5x for the 8+4 default) instead of replication's 3x.

The encode matrix is Cauchy-based: the top ``data`` rows are the identity
(systematic code) and each parity row is a Cauchy row  1/(x_j ^ y_i)  over
GF(2^8).  Cauchy rows keep every square submatrix invertible, so any k of
the k+m shards decode.  Fast-path multiplies run through
``bytes.translate`` + big-int XOR instead of per-byte Python loops.
"""

from __future__ import annotations

import functools

DATA_SHARDS = 8
PARITY_SHARDS = 4
STRIP = 8192  # shard-length quantum; keeps translate Big-Int ops chunky

_EXP = [0] * 512
_LOG = [0] * 256
_x = 1
for _i in range(255):
    _EXP[_i] = _x
    _LOG[_x] = _i
    _x <<= 1
    if _x & 0x100:
        _x ^= 0x11D  # primitive polynomial, generator 2
for _i in range(255, 512):
    _EXP[_i] = _EXP[_i - 255]


def gf_mul(a: int, b: int) -> int:
    if a == 0 or b == 0:
        return 0
    return _EXP[_LOG[a] + _LOG[b]]


def gf_inv(a: int) -> int:
    return _EXP[(255 - _LOG[a]) % 255]


@functools.cache
def _mul_table(a: int) -> bytes:
    return bytes(gf_mul(a, b) for b in range(256))


def _invert(rows: list[list[int]]) -> list[list[int]]:
    """Gauss-Jordan inverse of a k x k GF(256) matrix."""
    k = len(rows)
    a = [list(r) + [1 if i == j else 0 for j in range(k)] for i, r in enumerate(rows)]
    for col in range(k):
        piv = next((r for r in range(col, k) if a[r][col]), None)
        if piv is None:
            raise ValueError("encode matrix submatrix not invertible")
        a[col], a[piv] = a[piv], a[col]
        pv = gf_inv(a[col][col])
        if pv != 1:
            a[col] = [gf_mul(pv, v) for v in a[col]]
        for r in range(k):
            if r != col and a[r][col]:
                f = a[r][col]
                a[r] = [v ^ gf_mul(f, w) for v, w in zip(a[r], a[col])]
    return [row[k:] for row in a]


class InsufficientShards(ValueError):
    """Fewer than `data` shards were available to decode."""


class ErasureCodec:
    def __init__(self, data_shards: int = DATA_SHARDS, parity_shards: int = PARITY_SHARDS):
        if data_shards > 128 or parity_shards > 128:
            raise ValueError("shard counts must stay below GF field sanity limits")
        self.data = data_shards
        self.parity = parity_shards
        self.total = data_shards + parity_shards
        self.matrix = self._matrix()

    def _matrix(self) -> list[list[int]]:
        """Full (k+m) x k matrix: identity rows, then Cauchy parity rows."""
        m, k = self.parity, self.data
        xs = list(range(m))          # x_j = j      (parity row evals)
        ys = list(range(m, m + k))   # y_i = m + i  (data col evals)
        rows = []
        for i in range(k):           # systematic identity block
            rows.append([1 if c == i else 0 for c in range(k)])
        for x_j in xs:               # Cauchy block
            rows.append([gf_inv(x_j ^ y_i) for y_i in ys])
        return rows

    # ---- encode -------------------------------------------------------

    def encode(self, chunk: bytes) -> list[bytes]:
        """-> total shards of equal length. Shard 0..data-1 = data blocks.
        The first 4 bytes of the padded payload store the true length."""
        payload = len(chunk).to_bytes(4, "big") + chunk
        k = self.data
        if len(chunk) == 0:
            L = 1
        else:
            L = -(-len(payload) // k)
            L = max(1, ((L + STRIP - 1) // STRIP) * STRIP)
        if L * k < len(payload):
            L += STRIP
        padded = payload + b"\x00" * (L * k - len(payload))
        dshards = [padded[i * L : (i + 1) * L] for i in range(k)]
        return dshards + self._parity_from(dshards)

    def _parity_from(self, dshards: list[bytes]) -> list[bytes]:
        L = len(dshards[0])
        out = []
        for j in range(self.parity):
            acc = 0
            row = self.matrix[self.data + j]
            for i, ds in enumerate(dshards):
                coef = row[i]
                if coef:
                    acc ^= int.from_bytes(ds.translate(_mul_table(coef)), "big")
            out.append(acc.to_bytes(L, "big") if L < (1 << 30) else acc.to_bytes((L + 7) // 8, "big"))
        return out

    def regenerate_parity(self, data_shards: list[bytes]) -> list[bytes]:
        """Rebuild parity shards from surviving data shards (repair path)."""
        if len(data_shards) != self.data:
            raise ValueError("need all data shards to regenerate parity")
        return self._parity_from(data_shards)

    # ---- decode -------------------------------------------------------

    def decode(self, shards: list[bytes | None]) -> bytes:
        """Take k+m shards (None = lost); ANY data shards reconstruct."""
        present = [(i, s) for i, s in enumerate(shards) if s]
        if len(present) < self.data:
            raise InsufficientShards(
                f"need {self.data} shards, only {len(present)} present"
            )
        present = present[: self.data]
        rows = [self.matrix[i] for i, _ in present]
        inv = _invert(rows)
        L = len(present[0][1])
        if any(len(s) != L for _, s in present):
            raise ValueError("shards disagree on length — compute mismatch")
        data = []
        for r in range(self.data):
            acc = 0
            for c, (_, s) in enumerate(present):
                coef = inv[r][c]
                if coef:
                    acc ^= int.from_bytes(s.translate(_mul_table(coef)), "big")
            data.append(acc.to_bytes(L, "big"))
        joined = b"".join(data)
        ln = int.from_bytes(joined[:4], "big")
        if 4 + ln > len(joined):
            raise ValueError("corrupt shards: declared length exceeds payload")
        return joined[4 : 4 + ln]
