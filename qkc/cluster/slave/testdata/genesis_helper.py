#!/usr/bin/env python3
# Copyright 2026-2027, QuarkChain.
"""
Compute genesis block hash and serialized header from cluster_config.json.

Outputs two hex lines to stdout:
  line 1: genesis_root_block_hash (32 bytes)
  line 2: serialized RootBlockHeader
"""

import json
import os
import socket
import sys
import warnings

# Suppress urllib3 OpenSSL warnings on macOS
warnings.filterwarnings("ignore")

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

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.genesis import GenesisManager


def main():
    if len(sys.argv) < 2:
        print("usage: genesis_helper.py <cluster_config.json>", file=sys.stderr)
        sys.exit(1)

    config_path = sys.argv[1]

    with open(config_path) as f:
        config = ClusterConfig.from_json(f.read())
        config.json_filepath = config_path

    gm = GenesisManager(config.QUARKCHAIN)
    genesis_block = gm.create_root_block()
    genesis_hash = genesis_block.header.get_hash()
    header_bytes = genesis_block.header.serialize()

    print(genesis_hash.hex())
    print(header_bytes.hex())


if __name__ == "__main__":
    main()