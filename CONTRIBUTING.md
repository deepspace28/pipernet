# ⚡ Contributing to PiperNet

Thank you for using PiperNet and for wanting to make it better! All
contributions are welcome — code, bug reports, ideas, documentation, or
just spreading the word about the project. 🔥

- **Report Bugs**: Identify and resolve issues with the existing codebase.
- **Submit Ideas**: Request new features or share enhancements you'd like to see.
- **Develop Features**: Implement new functionality or improve existing tools via PRs.
- **Improve Documentation**: Help by improving the README, guides, or docstrings.
- **Test on Your Machine**: Run the demo and tests on your platform and report what breaks.

One of the best ways to support the project is to run
`py demo/resiliency_demo.py --erasure`, watch nodes die mid-stream and
the network heal itself, then tell people about it. A star on the repo
goes a long way too. ⭐

## Project rules (read before coding)

PiperNet has a hard constraint that shapes every contribution:

1. **Python 3.10+ standard library only.** No third-party runtime
   dependencies in the core (`pipernet/`) or the demo (`demo/`). This is
   the whole point of the project — a P2P network you can run with a
   bare `py` install. Dev-only extras for tests are fine if they earn it.
2. **Crypto changes must be RFC-vector-verified.** Anything touching
   `pipernet/crypto.py` (X25519 / RFC 7748, ChaCha20-Poly1305 / RFC 8439,
   HKDF / RFC 5869) must pass the official RFC test vectors, and new
   vectors for edge cases should be added alongside the change. Do not
   invent your own cipher constructions.
3. **Async everything on the wire.** Peer IO is asyncio TCP
   (length-prefixed JSON + payload). Keep handlers non-blocking; the
   repair loop and peer handler share one event loop.
4. **Content addressing is sacred.** Chunks are addressed by SHA-256
   CIDs and verified on receipt. Any change to `chunking.py`,
   `erasure.py`, or the store/fetch ops must keep the
   put -> chunk/CID -> spread -> fetch -> verify round-trip exact.

## Setting up a dev environment

```bash
git clone https://github.com/deepspace28/pipernet
cd pipernet

# no runtime deps — stdlib only. For the test suite you need pytest itself
# (dev-only: pip install pytest), then:
py -m pytest tests/ -v

# run the resiliency demo (replication mode)
py demo/resiliency_demo.py

# erasure-coded mode (Reed-Solomon 8+4)
py demo/resiliency_demo.py --erasure
```

## Submitting Issues

If you find a bug or have a feature idea, we'd love to hear from you!

### Reporting Bugs

1. **Search First**: Check if the issue has already been reported using
   GitHub's search bar under Issues.
2. **Details Matter**: Include your OS, Python version (`py --version`),
   and which mode you were running (`replication` or `--erasure`,
   `secure=True` or not). For wire/repair bugs, paste the full traceback.
3. **Reproduce It**: A minimal snippet or a tweaked `demo/resiliency_demo.py`
   invocation that reproduces the issue is incredibly helpful.

### Feature Ideas

Open an issue describing the problem you're solving before writing code
for anything that touches the wire protocol or the erasure math — the
protocol and the codec are the two places where small changes have large
consequences.

## Pull Request Guidelines

- Keep PRs focused on a **single change**.
- Include a concise description and motivation, and link related issues.
- **Add tests.** Every fix or feature lands with a test in `tests/` that
  fails without your change. Crypto PRs must include RFC-vector tests.
- Run the full suite before opening: `py -m pytest tests/ -v` — green or
  it doesn't go up.
- Run the demo in both modes if you touched `node.py`, `protocol.py`,
  or `erasure.py`:

  ```bash
  py demo/resiliency_demo.py && py demo/resiliency_demo.py --erasure
  ```

- Match the existing code style: stdlib-first, type hints on public
  functions, no comments narrating obvious code.

## Where to start

Good first contributions:

- Documentation and docstring polish (modules are lightly documented).
- More RFC test vectors / edge-case tests in `tests/test_crypto.py` and
  `tests/test_erasure.py`.
- Dashboard UX fixes in `pipernet/dashboard.py` (it's a tiny hand-rolled
  HTTP server — plenty of rough edges).
- Roadmap items listed in the README (wire-level session encryption
  integration, distributed origins, keypair identity) — discuss first.

Thank you for reading, and have fun building the middle-out future! ⚡
