#!/usr/bin/env python3
"""Python Master protocol peer for PeerConn interoperability tests.

This peer simulates a Python Master that:
  1. Accepts a Go Slave connection (MasterConn)
  2. Performs PING/PONG handshake
  3. Sends CreateClusterPeerConnectionRequest to create a PeerConn on the Go side
  4. Sends peer traffic (CommandOp frames with cluster_peer_id != 0) through
     the MasterConn transport, simulating forwarded external peer traffic
  5. Validates that the Go PeerConn correctly handles the traffic
  6. Sends DestroyClusterPeerConnectionCommand to tear down the PeerConn
  7. Verifies the connection is still alive after destroy

Wire format for master frames:
  [4B payload_len][4B branch][8B cluster_peer_id][1B opcode][8B rpc_id][payload]

Usage:
    python3 peer_master.py --port 0 --id "py-master" --shards "1,2" --cluster-peer-id 42

Output:
    PORT:<port>
    PONG_OK id=<hex>
    CREATE_OK error_code=<n>
    PEER_RPC_OK opcode=0x0a rpc_id=<n>
    PEER_NONRPC_OK
    DESTROY_OK
    POST_DESTROY_PONG_OK id=<hex>
    DISCONNECTED
"""
import argparse
import socket
import struct
import sys

from master_frame import read_master_frame, write_master_frame
from messages import serialize_ping_request, parse_pong_response

CLUSTER_OP_BASE = 0x80

# Cluster opcodes (master <-> slave)
CLUSTER_OP_PING = 1 + CLUSTER_OP_BASE
CLUSTER_OP_PONG = 2 + CLUSTER_OP_BASE
CLUSTER_OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST = 25 + CLUSTER_OP_BASE
CLUSTER_OP_CREATE_CLUSTER_PEER_CONNECTION_RESPONSE = 26 + CLUSTER_OP_BASE
CLUSTER_OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND = 27 + CLUSTER_OP_BASE

# Command opcodes (peer <-> peer, tunneled through master)
COMMAND_OP_GET_MINOR_BLOCK_LIST_REQUEST = 0x09
COMMAND_OP_GET_MINOR_BLOCK_LIST_RESPONSE = 0x0A
COMMAND_OP_NEW_MINOR_BLOCK_HEADER_LIST = 0x01


def serialize_create_peer_connection_request(cluster_peer_id):
    """Serialize CreateClusterPeerConnectionRequest.

    Fields:
      ClusterPeerID: uint64 (8 bytes BE)
    """
    return struct.pack('>Q', cluster_peer_id)


def serialize_destroy_peer_connection_command(cluster_peer_id):
    """Serialize DestroyClusterPeerConnectionCommand.

    Fields:
      ClusterPeerID: uint64 (8 bytes BE)
    """
    return struct.pack('>Q', cluster_peer_id)


def serialize_get_minor_block_list_request():
    """Serialize GetMinorBlockListRequest.

    Fields:
      MinorBlockHashList: [][32]byte (4B count + hashes)
    """
    # Empty hash list
    return struct.pack('>I', 0)


def serialize_new_minor_block_header_list_command():
    """Serialize NewMinorBlockHeaderListCommand.

    Fields:
      RootBlockHeader: *RawBytes (nil marker 0x00)
      MinorBlockHeaderList: []*RawBytes (4B count + items)
    """
    data = b'\x00'  # RootBlockHeader: nil
    data += struct.pack('>I', 0)  # empty list
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--id', type=str, required=True)
    parser.add_argument('--shards', type=str, required=True)
    parser.add_argument('--cluster-peer-id', type=int, required=True)
    args = parser.parse_args()

    master_id = args.id.encode('utf-8')
    shard_list = [int(s) for s in args.shards.split(',')]
    cluster_peer_id = args.cluster_peer_id
    branch = 0x00010001  # shard_id=1, chain_size=1

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', args.port))
    server.listen(1)

    actual_port = server.getsockname()[1]
    print(f"PORT:{actual_port}", flush=True)

    conn, _ = server.accept()

    try:
        # 1. PING -> PONG (verify MasterConn is alive)
        _send_ping(conn, master_id, shard_list)

        # 2. CreateClusterPeerConnectionRequest -> Response
        create_payload = serialize_create_peer_connection_request(cluster_peer_id)
        write_master_frame(
            conn,
            CLUSTER_OP_CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
            2,  # rpc_id
            create_payload,
            branch=branch,
        )

        frame = read_master_frame(conn)
        if frame is None:
            print("ERROR: no response for create peer connection", flush=True)
            sys.exit(1)

        if frame['opcode'] != CLUSTER_OP_CREATE_CLUSTER_PEER_CONNECTION_RESPONSE:
            print(f"ERROR: expected CREATE_RESPONSE(0x{CLUSTER_OP_CREATE_CLUSTER_PEER_CONNECTION_RESPONSE:02x}), "
                  f"got 0x{frame['opcode']:02x}", flush=True)
            sys.exit(1)

        if frame['rpc_id'] != 2:
            print(f"ERROR: expected rpc_id 2, got {frame['rpc_id']}", flush=True)
            sys.exit(1)

        error_code = struct.unpack('>I', frame['payload'][0:4])[0]
        print(f"CREATE_OK error_code={error_code}", flush=True)

        # 3. Send peer RPC traffic (cluster_peer_id != 0)
        #    GetMinorBlockListRequest -> GetMinorBlockListResponse
        peer_rpc_payload = serialize_get_minor_block_list_request()
        write_master_frame(
            conn,
            COMMAND_OP_GET_MINOR_BLOCK_LIST_REQUEST,
            100,  # peer rpc_id
            peer_rpc_payload,
            branch=branch,
            cluster_peer_id=cluster_peer_id,
        )

        frame = read_master_frame(conn)
        if frame is None:
            print("ERROR: no response for peer RPC", flush=True)
            sys.exit(1)

        if frame['opcode'] != COMMAND_OP_GET_MINOR_BLOCK_LIST_RESPONSE:
            print(f"ERROR: expected peer response 0x{COMMAND_OP_GET_MINOR_BLOCK_LIST_RESPONSE:02x}, "
                  f"got 0x{frame['opcode']:02x}", flush=True)
            sys.exit(1)

        if frame['rpc_id'] != 100:
            print(f"ERROR: expected peer rpc_id 100, got {frame['rpc_id']}", flush=True)
            sys.exit(1)

        # Verify response metadata has correct cluster_peer_id
        if frame['cluster_peer_id'] != cluster_peer_id:
            print(f"ERROR: expected response cluster_peer_id {cluster_peer_id}, "
                  f"got {frame['cluster_peer_id']}", flush=True)
            sys.exit(1)

        print(f"PEER_RPC_OK opcode=0x{frame['opcode']:02x} rpc_id={frame['rpc_id']}", flush=True)

        # 4. Send peer non-RPC traffic (fire-and-forget)
        nonrpc_payload = serialize_new_minor_block_header_list_command()
        write_master_frame(
            conn,
            COMMAND_OP_NEW_MINOR_BLOCK_HEADER_LIST,
            0,  # non-RPC: rpc_id must be 0
            nonrpc_payload,
            branch=branch,
            cluster_peer_id=cluster_peer_id,
        )

        # Non-RPC produces no response. Verify by sending a follow-up PING.
        _send_ping(conn, master_id, shard_list, rpc_id=3)
        print("PEER_NONRPC_OK", flush=True)

        # 5. DestroyClusterPeerConnectionCommand (fire-and-forget)
        destroy_payload = serialize_destroy_peer_connection_command(cluster_peer_id)
        write_master_frame(
            conn,
            CLUSTER_OP_DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
            0,  # non-RPC
            destroy_payload,
            branch=branch,
        )
        print("DESTROY_OK", flush=True)

        # 6. Verify MasterConn is still alive after destroy
        _send_ping(conn, master_id, shard_list, rpc_id=4)

    except (ConnectionError, BrokenPipeError, OSError) as e:
        print(f"ERROR: {e}", flush=True)
        sys.exit(1)
    finally:
        conn.close()
        server.close()
        print("DISCONNECTED", flush=True)


def _send_ping(conn, master_id, shard_list, rpc_id=1):
    """Send PING, wait for PONG, validate and print result."""
    ping_payload = serialize_ping_request(master_id, shard_list)
    write_master_frame(conn, CLUSTER_OP_PING, rpc_id, ping_payload)

    frame = read_master_frame(conn)
    if frame is None:
        print(f"ERROR: no pong received for rpc_id={rpc_id}", flush=True)
        sys.exit(1)

    if frame['opcode'] != CLUSTER_OP_PONG:
        print(f"ERROR: expected PONG(0x{CLUSTER_OP_PONG:02x}), got 0x{frame['opcode']:02x}", flush=True)
        sys.exit(1)

    if frame['rpc_id'] != rpc_id:
        print(f"ERROR: expected rpc_id {rpc_id}, got {frame['rpc_id']}", flush=True)
        sys.exit(1)

    peer_id_recv, _ = parse_pong_response(frame['payload'])
    if rpc_id == 1:
        print(f"PONG_OK id={peer_id_recv.hex()}", flush=True)
    else:
        print(f"POST_DESTROY_PONG_OK id={peer_id_recv.hex()}", flush=True)


if __name__ == '__main__':
    main()
