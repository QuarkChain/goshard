#!/usr/bin/env python3
"""Minimal Python master helper for Go Slave integration tests.

This script uses the real pyquarkchain protocol classes without importing
quarkchain.cluster.master (which depends on native mining libraries). It acts
as a lightweight master-side endpoint to exercise the Go Slave runtime.
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
from quarkchain.core import Branch
from quarkchain.cluster.rpc import (
    CLUSTER_OP_SERIALIZER_MAP,
    ClusterOp,
    ConnectToSlavesRequest,
    CreateClusterPeerConnectionRequest,
    DestroyClusterPeerConnectionCommand,
    Ping,
    SlaveInfo,
)
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
    """A master-side connection to a Go Slave.

    It does not forward slave->peer traffic because the integration tests only
    exercise master->slave forwarding.
    """

    def __init__(self, reader, writer, full_shard_id_list, name=None):
        super().__init__(
            DummyEnv,
            reader,
            writer,
            OP_SER_MAP,
            {},  # op_non_rpc_map
            {},  # op_rpc_map
            name=name,
        )
        self.full_shard_id_list = full_shard_id_list
        self._loop_task = asyncio.create_task(self.active_and_loop_forever())

    def get_connection_to_forward(self, metadata):
        return None

    async def shutdown(self):
        if self.state != ConnectionState.CLOSED:
            self.close()
        await self.wait_until_closed()


async def open_connection(host, port, full_shard_id_list, name=None):
    reader, writer = await asyncio.open_connection(host, port)
    conn = MasterToSlaveConnection(reader, writer, full_shard_id_list, name=name)
    await conn.wait_until_active()
    return conn


def parse_shards(s):
    return [int(x.strip(), 0) for x in s.split(",") if x.strip()]


async def cmd_ping(args):
    conn = await open_connection(args.host, args.port, args.master_shards, name="py-master-ping")
    try:
        req = Ping(b"", [], None)
        op, resp, rpc_id = await conn.write_rpc_request(ClusterOp.PING, req)
        print(f"PONG id={resp.id.hex()} shards={','.join(str(x) for x in resp.full_shard_id_list)}")
        return 0
    finally:
        await conn.shutdown()


async def cmd_peer(args):
    conn = await open_connection(args.host, args.port, args.master_shards, name="py-master-peer")
    try:
        # Handshake: master sends PING, slave replies PONG.
        req = Ping(b"", [], None)
        op, resp, rpc_id = await conn.write_rpc_request(ClusterOp.PING, req)
        print(f"PONG id={resp.id.hex()} shards={','.join(str(x) for x in resp.full_shard_id_list)}")

        # Create virtual peer connections.
        create_req = CreateClusterPeerConnectionRequest(args.cluster_peer_id)
        op, create_resp, rpc_id = await conn.write_rpc_request(
            ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST, create_req
        )
        print(f"CREATE_PEER error_code={create_resp.error_code}")
        if create_resp.error_code != 0:
            return 1

        # Route a non-RPC peer command through the master to the slave.
        # The frame has cluster_peer_id != 0 so the slave must forward it to
        # the virtual PeerConn instead of handling it as a master RPC.
        peer_cmd = NewTransactionListCommand([])
        metadata = ClusterMetadata(
            branch=Branch(args.master_shards[0]), cluster_peer_id=args.cluster_peer_id
        )
        conn.write_command(CommandOp.NEW_TRANSACTION_LIST, peer_cmd, rpc_id=0, metadata=metadata)
        print("PEER_ROUTE sent")

        # Give the slave a moment to process the forwarded command.
        await asyncio.sleep(0.1)

        # Destroy the virtual peer connections.
        destroy_cmd = DestroyClusterPeerConnectionCommand(args.cluster_peer_id)
        conn.write_command(
            ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND, destroy_cmd, rpc_id=0
        )
        print("DESTROY_PEER sent")
        return 0
    finally:
        await conn.shutdown()


async def cmd_xshard(args):
    """Connect to two Go slaves as master and instruct them to connect to each other."""
    conn1 = None
    conn2 = None
    try:
        conn1 = await open_connection(
            args.slave1_host, args.slave1_port, args.master_shards, name="py-master-xshard-1"
        )
        conn2 = await open_connection(
            args.slave2_host, args.slave2_port, args.master_shards, name="py-master-xshard-2"
        )
        # Handshake with both slaves.
        ping_req = Ping(b"", [], None)
        op1, resp1, _ = await conn1.write_rpc_request(ClusterOp.PING, ping_req)
        op2, resp2, _ = await conn2.write_rpc_request(ClusterOp.PING, ping_req)
        print(
            f"PONG1 id={resp1.id.hex()} shards={','.join(str(x) for x in resp1.full_shard_id_list)}"
        )
        print(
            f"PONG2 id={resp2.id.hex()} shards={','.join(str(x) for x in resp2.full_shard_id_list)}"
        )

        slave1_id = bytes(args.slave1_id, "ascii")
        slave2_id = bytes(args.slave2_id, "ascii")

        # Tell slave1 to connect to slave2.
        info_for_slave1 = [
            SlaveInfo(
                slave2_id,
                bytes(args.slave2_host, "ascii"),
                args.slave2_port,
                args.slave2_shards,
            )
        ]
        req1 = ConnectToSlavesRequest(info_for_slave1)
        op, connect_resp1, _ = await conn1.write_rpc_request(
            ClusterOp.CONNECT_TO_SLAVES_REQUEST, req1
        )
        errors1 = [r for r in connect_resp1.result_list if r]
        print(f"CONNECT_SLAVE1 results={len(connect_resp1.result_list)} errors={len(errors1)}")
        for i, err in enumerate(errors1):
            print(f"  slave1 result[{i}] error={err.decode('utf-8', errors='replace')}")

        # Tell slave2 to connect to slave1.
        info_for_slave2 = [
            SlaveInfo(
                slave1_id,
                bytes(args.slave1_host, "ascii"),
                args.slave1_port,
                args.slave1_shards,
            )
        ]
        req2 = ConnectToSlavesRequest(info_for_slave2)
        op, connect_resp2, _ = await conn2.write_rpc_request(
            ClusterOp.CONNECT_TO_SLAVES_REQUEST, req2
        )
        errors2 = [r for r in connect_resp2.result_list if r]
        print(f"CONNECT_SLAVE2 results={len(connect_resp2.result_list)} errors={len(errors2)}")
        for i, err in enumerate(errors2):
            print(f"  slave2 result[{i}] error={err.decode('utf-8', errors='replace')}")

        # Allow outbound dials and PING/PONG handshakes to complete.
        await asyncio.sleep(0.5)

        if errors1 or errors2:
            return 1
        print("XSHARD_OK", flush=True)
        # Keep the master connections open long enough for the Go test to
        # inspect the xshard pool before teardown begins.
        await asyncio.sleep(args.linger)
        return 0
    finally:
        if conn1 is not None:
            await conn1.shutdown()
        if conn2 is not None:
            await conn2.shutdown()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Go Slave integration test helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ping_p = subparsers.add_parser("ping", help="Ping a Go slave")
    ping_p.add_argument("host")
    ping_p.add_argument("port", type=int)
    ping_p.add_argument("master_shards", help="comma-separated full shard ids")

    peer_p = subparsers.add_parser("peer", help="Create/route/destroy a peer connection")
    peer_p.add_argument("host")
    peer_p.add_argument("port", type=int)
    peer_p.add_argument("master_shards", help="comma-separated full shard ids")
    peer_p.add_argument("cluster_peer_id", type=int)

    xshard_p = subparsers.add_parser("xshard", help="Connect two Go slaves to each other")
    xshard_p.add_argument("slave1_host")
    xshard_p.add_argument("slave1_port", type=int)
    xshard_p.add_argument("slave2_host")
    xshard_p.add_argument("slave2_port", type=int)
    xshard_p.add_argument("master_shards", help="comma-separated full shard ids")
    xshard_p.add_argument("slave1_id")
    xshard_p.add_argument("slave1_shards", help="comma-separated full shard ids")
    xshard_p.add_argument("slave2_id")
    xshard_p.add_argument("slave2_shards", help="comma-separated full shard ids")
    xshard_p.add_argument(
        "--linger",
        type=float,
        default=2.0,
        help="seconds to keep master connections open after XSHARD_OK",
    )

    args = parser.parse_args()
    args.master_shards = parse_shards(args.master_shards)
    if not args.master_shards:
        print("ERROR: master_shards must not be empty", file=sys.stderr)
        sys.exit(1)
    if args.command == "xshard":
        args.slave1_shards = parse_shards(args.slave1_shards)
        args.slave2_shards = parse_shards(args.slave2_shards)

    if args.command == "ping":
        rc = asyncio.run(cmd_ping(args))
    elif args.command == "peer":
        rc = asyncio.run(cmd_peer(args))
    elif args.command == "xshard":
        rc = asyncio.run(cmd_xshard(args))
    else:
        parser.print_help()
        rc = 2

    sys.exit(rc)


if __name__ == "__main__":
    main()
