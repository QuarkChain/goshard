#!/usr/bin/env python3
# Copyright 2026-2027, QuarkChain.
"""
Bootstrap test wrapper for master.py.

Patches the native qkchash library (libqkchash.so) with Python stubs so that
the master can start without the native library. The native library is only
needed for actual PoW mining, which is not required for bootstrap smoke tests.

This is a test helper, NOT a modification to pyquarkchain source code.
"""

import os
import socket
import sys

# ── Patch hostname resolution (macOS may not resolve .local hostnames) ───────
_original_gethostname = socket.gethostname
_original_gethostbyname = socket.gethostbyname


def _patched_gethostname():
    return "localhost"


def _patched_gethostbyname(name):
    if name == "localhost":
        return "127.0.0.1"
    return _original_gethostbyname(name)


socket.gethostname = _patched_gethostname
socket.gethostbyname = _patched_gethostbyname

# ── Resolve pyquarkchain root ────────────────────────────────────────────────
_pyquarkchain_root = os.environ.get("PYQUARKCHAIN", "")
if _pyquarkchain_root:
    sys.path.insert(0, _pyquarkchain_root)
else:
    print("PYQUARKCHAIN not set", file=sys.stderr)
    sys.exit(1)

# ── Stub qkchash.qkchash (avoids loading libqkchash.so) ─────────────────────
class _StubQkcHashNative:
    """Stub that replaces the native QkcHashNative."""
    def __init__(self, lib_path=None):
        pass
    def hash(self, *args, **kwargs):
        raise NotImplementedError("qkchash native library not available")
    def mine(self, *args, **kwargs):
        raise NotImplementedError("qkchash native library not available")


class _StubQkchash:
    """Stub module replacing qkchash.qkchash."""
    QkcHashNative = _StubQkcHashNative

sys.modules["qkchash.qkchash"] = _StubQkchash()

# ── Stub qkchash.qkcpow (avoids init_qkc_hash_native) ───────────────────────
_stub_native = _StubQkcHashNative()

class _StubQkchashMiner:
    """Stub replacing qkchash.qkcpow.QkchashMiner."""
    def __init__(self, qkc_hash_native=None):
        pass
    def mine(self, *args, **kwargs):
        raise NotImplementedError("qkchash miner not available")
    def check_pow(self, *args, **kwargs):
        return False


def _stub_check_pow(header_hash, nonce, boundary, qkc_hash_native):
    return False


class _StubQkcpow:
    """Stub module replacing qkchash.qkcpow."""
    QkchashMiner = _StubQkchashMiner
    check_pow = _stub_check_pow
    QKC_HASH_NATIVE = _stub_native

sys.modules["qkchash.qkcpow"] = _StubQkcpow()

# ── Create qkchash package stub ──────────────────────────────────────────────
class _StubQkchashPackage:
    pass

sys.modules["qkchash"] = _StubQkchashPackage()

# ── Force SimpleNetwork mode ──────────────────────────────────────────────────
# We need the master to use SimpleNetwork (not P2PManager/RLPx) so that
# interop tests can connect a minimal external peer via a simple HELLO
# handshake.  Monkey-patching use_p2p() is the least invasive way to
# switch the network backend without modifying pyquarkchain source.
from quarkchain.cluster.cluster_config import ClusterConfig

ClusterConfig.use_p2p = lambda self: False

# ── Run master.main() ────────────────────────────────────────────────────────
from quarkchain.cluster.master import main
main()