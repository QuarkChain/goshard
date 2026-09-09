#!/usr/bin/env python3
# Copyright 2026-2027, QuarkChain.
"""Single Python driver for Go Slave interop tests.

Unified entry point backed by the *real* pyquarkchain wire stack. It never
reimplements the wire protocol — it reuses ClusterConnection, the cluster OP
serializer map and the real RPC request classes, and only performs the
test-scripted orchestration on top.

Sub-commands:

  bootstrap   Run the full Python master (master.main()[mounted]).
  rpc         Connect to one Go slave, PING, then round-trip a business RPC.
  peer        Connect to one Go slave, drive CREATE/DESTROY cluster peer.
  disconnect  Connect to one Go slave, PING, then close the connection.

The heavy bootstrap path (master.main + native qkchash) is loaded lazily only
when the "bootstrap" sub-command is used, so the lighter scenario paths do not
need the native mining libraries.
"""
import asyncio
import os
import sys

pyquarkchain_path = os.environ.get("PYQUARKCHAIN")
if not pyquarkchain_path:
    print("ERROR: PYQUARKCHAIN environment variable is not set", file=sys.stderr)
    sys.exit(1)
sys.path.insert(0, pyquarkchain_path)

from quarkchain.cluster.p2p_commands import (
    CommandOp,
    NewTransactionListCommand,
    OP_SERIALIZER_MAP as P2P_OP_SERIALIZER_MAP,
)
from quarkchain.cluster.protocol import ClusterConnection, ClusterMetadata
from quarkchain.cluster.rpc import (
    CLUSTER_OP_SERIALIZER_MAP,
    ClusterOp,
    AddMinorBlockHeaderResponse,
    ArtificialTxConfig,
    CreateClusterPeerConnectionRequest,
    DestroyClusterPeerConnectionCommand,
    GetAccountDataRequest,
    Ping,
)
from quarkchain.core import Address, Branch
from quarkchain.protocol import ConnectionState


def _merge_op_maps():
    merged = dict(CLUSTER_OP_SERIALIZER_MAP)
    merged.update(P2P_OP_SERIALIZER_MAP)
    return merged


OP_SER_MAP = _merge_op_maps()


class DummyEnv:
    """Minimal environment object required by quarkchain.protocol.Connection."""

    class cluster_config:
        @staticmethod
        def get_slave_command_size_limit():
            return None


class MasterToSlaveConnection(ClusterConnection):
    """Master-side connection to a Go Slave.

    Reuses the real ClusterConnection wire stack with the real cluster OP
    serializer map. It does not forward slave->peer traffic because the
    scenario tests only exercise master->slave orchestration.

    ADD_MINOR_BLOCK_HEADER_REQUEST is answered here (not forwarded to a real
    MasterServer) because this harness tests Go↔Python wire compatibility, not
    Python block-processing. The real request serializer performs the decode
    and the real AddMinorBlockHeaderResponse class performs the reply encode.
    """

    def __init__(self, reader, writer, name=None):
        super().__init__(
            DummyEnv,
            reader,
            writer,
            OP_SER_MAP,
            {},  # op_non_rpc_map
            {
                ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: (
                    ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE,
                    _handle_add_minor_block_header,
                ),
            },  # op_rpc_map
            name=name,
        )
        self._loop_task = asyncio.create_task(self.active_and_loop_forever())

    def get_connection_to_forward(self, metadata):
        return None

    async def shutdown(self):
        if self.state != ConnectionState.CLOSED:
            self.close()
        await self.wait_until_closed()


async def open_connection(host, port, name=None):
    reader, writer = await asyncio.open_connection(host, port)
    conn = MasterToSlaveConnection(reader, writer, name=name)
    await conn.wait_until_active()
    return conn


def parse_shards(s):
    return [int(x.strip(), 0) for x in s.split(",") if x.strip()]


# ---------------------------------------------------------------------------
# Scenario sub-commands
# ---------------------------------------------------------------------------


async def do_rpc(args):
    conn = await open_connection(args.host, args.port, name="py-master-rpc")
    try:
        req = Ping(b"", [], None)
        await conn.write_rpc_request(ClusterOp.PING, req)

        # full_shard_key=0 addresses the root shard; the Go slave's
        # communication-only backend returns success for any address.
        address = Address(bytes.fromhex(args.address_hex), full_shard_key=0)
        req = GetAccountDataRequest(address)
        op, resp, _ = await conn.write_rpc_request(ClusterOp.GET_ACCOUNT_DATA_REQUEST, req)
        print(f"RPC_OK error_code={resp.error_code}", flush=True)
        return 0 if resp.error_code == 0 else 1
    finally:
        await conn.shutdown()


async def do_peer(args):
    conn = await open_connection(args.host, args.port, name="py-master-peer")
    try:
        # Handshake: master sends PING, slave replies PONG.
        req = Ping(b"", [], None)
        op, resp, _ = await conn.write_rpc_request(ClusterOp.PING, req)

        # Create two virtual cluster peer connections, then destroy only the
        # first so the Go test can assert destroy(peer1) leaves peer2 intact.
        for peer_id in args.cluster_peer_ids:
            create_req = CreateClusterPeerConnectionRequest(peer_id)
            op, create_resp, _ = await conn.write_rpc_request(
                ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST, create_req
            )
            if create_resp.error_code != 0:
                print(f"CREATE_PEER error_code={create_resp.error_code}", file=sys.stderr, flush=True)
                return 1
            print(f"PEER_CREATED {peer_id}", flush=True)
            # Leave a window for the Go test to observe each peer before the next.
            await asyncio.sleep(0.5)

        # Destroy only the first peer (fire-and-forget), then leave a window for
        # the Go test to observe that peer2 survives peer1's destroy.
        destroy = DestroyClusterPeerConnectionCommand(args.cluster_peer_ids[0])
        conn.write_command(ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND, destroy, rpc_id=0)
        await asyncio.sleep(args.hold)
        print("PEER_DESTROYED", flush=True)
        return 0
    finally:
        await conn.shutdown()


async def do_peermsg(args):
    conn = await open_connection(args.host, args.port, name="py-master-peermsg")
    try:
        # Handshake.
        req = Ping(b"", [], None)
        op, resp, _ = await conn.write_rpc_request(ClusterOp.PING, req)

        # Create the virtual cluster peer connection the message is routed to.
        create_req = CreateClusterPeerConnectionRequest(args.cluster_peer_id)
        op, create_resp, _ = await conn.write_rpc_request(
            ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST, create_req
        )
        if create_resp.error_code != 0:
            print(f"CREATE_PEER error_code={create_resp.error_code}", file=sys.stderr, flush=True)
            return 1
        print(f"PEER_CREATED {args.cluster_peer_id}", flush=True)
        # Leave a window for the Go side to finish building the virtual PeerConn.
        await asyncio.sleep(args.wait)

        # Send a P2P command addressed to (cluster_peer_id, branch). The empty
        # transaction list serializes to a 4-byte zero length, byte-identical on
        # both sides, so it is a minimal wire-valid payload.
        cmd = NewTransactionListCommand()
        metadata = ClusterMetadata(branch=Branch(args.branch), cluster_peer_id=args.cluster_peer_id)
        conn.write_command(CommandOp.NEW_TRANSACTION_LIST, cmd, rpc_id=0, metadata=metadata)
        await asyncio.sleep(args.wait)
        print("PEER_MSG_SENT", flush=True)
        return 0
    finally:
        await conn.shutdown()


async def do_disconnect(args):
    conn = await open_connection(args.host, args.port, name="py-master-disconnect")
    try:
        req = Ping(b"", [], None)
        await conn.write_rpc_request(ClusterOp.PING, req)
        await asyncio.sleep(0.2)
        print("CONNECTED", flush=True)
        # Closing the connection is the master disconnecting; the Go slave must
        # observe it and shut down.
        await asyncio.sleep(0.2)
        await conn.shutdown()
        print("DISCONNECT_SENT", flush=True)
        return 0
    finally:
        await conn.shutdown()


# Set lazily per addminorblock run; the handler signals it once the Go slave's
# request has been received and a response has been written.
_add_minor_block_handled = None


async def _handle_add_minor_block_header(self, request):
    """Answer a Go slave's AddMinorBlockHeader request.

    Deliberately does NOT touch any MasterServer block-processing state. It
    decodes via the real request serializer (already done by the connection),
    echoes a real AddMinorBlockHeaderResponse, and lets the real RPC layer
    encode and send it back. Verifies Go encode → Python decode → Python
    encode → Go decode.
    """
    resp = AddMinorBlockHeaderResponse(
        error_code=0,
        artificial_tx_config=ArtificialTxConfig(
            target_root_block_time=10, target_minor_block_time=10
        ),
    )
    ev = _add_minor_block_handled
    if ev is not None:
        ev.set()
    return resp


async def do_add_minor_block(args):
    global _add_minor_block_handled
    conn = await open_connection(args.host, args.port, name="py-master-addminorblock")
    try:
        # Handshake: master sends PING and awaits the slave's PONG, confirming
        # the Go slave's MasterConn is active before signalling the test.
        req = Ping(b"", [], None)
        await conn.write_rpc_request(ClusterOp.PING, req)
        print("READY", flush=True)

        # Go slave now triggers SendMinorBlockHeaderToMaster; wait for the
        # request to arrive and our response to be written.
        _add_minor_block_handled = asyncio.Event()
        await asyncio.wait_for(_add_minor_block_handled.wait(), timeout=args.wait)
        # Give the response a moment to flush back to the Go slave.
        await asyncio.sleep(0.1)
        print("ADD_MINOR_BLOCK_OK error_code=0", flush=True)
        return 0
    finally:
        _add_minor_block_handled = None
        await conn.shutdown()


# ---------------------------------------------------------------------------
# Full-master sub-command (heavy, loaded lazily)
# ---------------------------------------------------------------------------


def _patch_hostname():
    """Route hostname lookups to localhost (macOS may not resolve .local names)."""
    import socket

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


def _stub_qkchash():
    """Stub the native qkchash library so master.main() runs without it.

    The native library is only needed for actual PoW mining, which is not part
    of a bootstrap smoke test.
    """

    class _StubQkcHashNative:
        def __init__(self, lib_path=None):
            pass

        def hash(self, *args, **kwargs):
            raise NotImplementedError("qkchash native library not available")

        def mine(self, *args, **kwargs):
            raise NotImplementedError("qkchash native library not available")

    class _StubQkchash:
        QkcHashNative = _StubQkcHashNative

    sys.modules["qkchash.qkchash"] = _StubQkchash()

    class _StubQkchashMiner:
        def __init__(self, qkc_hash_native=None):
            pass

        def mine(self, *args, **kwargs):
            raise NotImplementedError("qkchash miner not available")

        def check_pow(self, *args, **kwargs):
            return False

    def _stub_check_pow(header_hash, nonce, boundary, qkc_hash_native):
        return False

    class _StubQkcpow:
        QkchashMiner = _StubQkchashMiner
        check_pow = _stub_check_pow
        QKC_HASH_NATIVE = _StubQkcHashNative()

    sys.modules["qkchash.qkcpow"] = _StubQkcpow()
    sys.modules["qkchash"] = type("_StubQkchashPackage", (), {})()


def run_bootstrap():
    """Patch environment, then run the real Python master. Uses the master's
    own argument parser, so script argv is rewritten to drop the "bootstrap"
    token and hand the remaining --cluster_config to master.main()."""
    _patch_hostname()
    _stub_qkchash()

    from quarkchain.cluster.cluster_config import ClusterConfig

    # Force SimpleNetwork so the master's P2P layer does not require RLPx.
    ClusterConfig.use_p2p = lambda self: False

    from quarkchain.cluster.master import main

    sys.exit(main())


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Go Slave interop master harness")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="run the full Python master (master.main())")

    for name in ("disconnect",):
        p = subparsers.add_parser(name)
        p.add_argument("host")
        p.add_argument("port", type=int)
        p.add_argument("master_shards")

    p = subparsers.add_parser("rpc")
    p.add_argument("host")
    p.add_argument("port", type=int)
    p.add_argument("master_shards")
    p.add_argument("address_hex")

    p = subparsers.add_parser("peer")
    p.add_argument("host")
    p.add_argument("port", type=int)
    p.add_argument("master_shards")
    p.add_argument("cluster_peer_ids", nargs="+", type=int)
    p.add_argument("--hold", type=float, default=1.0)

    p = subparsers.add_parser("peermsg")
    p.add_argument("host")
    p.add_argument("port", type=int)
    p.add_argument("master_shards")
    p.add_argument("cluster_peer_id", type=int)
    p.add_argument("branch", type=int)
    p.add_argument("--wait", type=float, default=2.0)

    p = subparsers.add_parser("addminorblock")
    p.add_argument("host")
    p.add_argument("port", type=int)
    p.add_argument("master_shards")
    p.add_argument("--wait", type=float, default=15.0)

    args, extras = parser.parse_known_args()

    if args.command == "bootstrap":
        # Ensure the master sees a clean argv: script name + its own args.
        sys.argv = [sys.argv[0]] + extras
        run_bootstrap()
        return

    args.master_shards = parse_shards(args.master_shards)
    if args.command == "rpc":
        rc = asyncio.run(do_rpc(args))
    elif args.command == "peer":
        rc = asyncio.run(do_peer(args))
    elif args.command == "peermsg":
        rc = asyncio.run(do_peermsg(args))
    elif args.command == "disconnect":
        rc = asyncio.run(do_disconnect(args))
    elif args.command == "addminorblock":
        rc = asyncio.run(do_add_minor_block(args))
    else:
        parser.print_help()
        rc = 2

    sys.exit(rc)


if __name__ == "__main__":
    main()