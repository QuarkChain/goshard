// Copyright 2026-2027, QuarkChain.

package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type PyDBSource struct {
	dbRoot        string
	clusterConfig string
	pythonPath    string
	pythonRoot    string
}

func NewPyDBSource(dbRoot string, clusterConfig ...string) *PyDBSource {
	pythonRoot := inferPyQuarkChainRoot(dbRoot)
	pythonPath := os.Getenv("PYQUARKCHAIN_PYTHON")
	if pythonPath == "" && pythonRoot != "" {
		candidate := filepath.Join(pythonRoot, ".venv", "bin", "python")
		if runtime.GOOS == "windows" {
			candidate = filepath.Join(pythonRoot, ".venv", "Scripts", "python.exe")
		}
		if _, err := os.Stat(candidate); err == nil {
			pythonPath = candidate
		}
	}
	if pythonPath == "" {
		pythonPath = "python3"
	}
	var configPath string
	if len(clusterConfig) != 0 {
		configPath = clusterConfig[0]
	}
	return &PyDBSource{dbRoot: dbRoot, clusterConfig: configPath, pythonPath: pythonPath, pythonRoot: pythonRoot}
}

func (s *PyDBSource) FetchMinorBlocks(fullShardKey uint32, start uint64, end uint64) ([]*MinorBlockInput, error) {
	if end < start {
		return nil, fmt.Errorf("invalid height range %d..%d", start, end)
	}
	fullShardID := fullShardKey
	script := pyDBExportScript
	cmd := exec.Command(s.pythonPath, "-c", script, s.dbRoot, fmt.Sprintf("%d", fullShardID), fmt.Sprintf("%d", start), fmt.Sprintf("%d", end), s.clusterConfig)
	if s.pythonRoot != "" {
		cmd.Dir = s.pythonRoot
		cmd.Env = append(os.Environ(), prependPythonPath(s.pythonRoot))
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	timer := time.AfterFunc(60*time.Second, func() {
		_ = cmd.Process.Kill()
	})
	out, err := cmd.Output()
	timer.Stop()
	if err != nil {
		return nil, fmt.Errorf("pyquarkchain db export failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var rawBlocks []json.RawMessage
	if err := json.Unmarshal(out, &rawBlocks); err != nil {
		return nil, err
	}
	blocks := make([]*MinorBlockInput, 0, len(rawBlocks))
	for i, raw := range rawBlocks {
		block, err := ParseMinorBlockInputJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("parse exported block %d: %w", i, err)
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func inferPyQuarkChainRoot(dbRoot string) string {
	dir := filepath.Clean(dbRoot)
	for {
		if _, err := os.Stat(filepath.Join(dir, "quarkchain", "core.py")); err == nil {
			return dir
		}
		if filepath.Base(dir) == "quarkchain" {
			if _, err := os.Stat(filepath.Join(dir, "core.py")); err == nil {
				return filepath.Dir(dir)
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func prependPythonPath(root string) string {
	const key = "PYTHONPATH"
	if existing := os.Getenv(key); existing != "" {
		return key + "=" + root + string(os.PathListSeparator) + existing
	}
	return key + "=" + root
}

const pyDBExportScript = `
import json
import os
import rlp
import sys

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.jsonrpc import data_encoder, minor_block_encoder
from quarkchain.cluster.neighbor import is_neighbor
from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.cluster.shard_state import XshardTxCursor
from quarkchain.core import Branch, CrossShardTransactionList, HashList, MinorBlock
from quarkchain.db import PersistentDb
from quarkchain.env import DEFAULT_ENV
from quarkchain.utils import token_id_encode

db_root = sys.argv[1]
full_shard_id = int(sys.argv[2], 0)
start_height = int(sys.argv[3], 0)
end_height = int(sys.argv[4], 0)
cluster_config_path = sys.argv[5] if len(sys.argv) > 5 else ""
db = PersistentDb(os.path.join(db_root, "shard-{}.db".format(full_shard_id)))

cluster_config = None
env = None
branch = None
shard_db = None
cursor_shard_state = None

if cluster_config_path:
    with open(cluster_config_path) as f:
        cluster_config = ClusterConfig.from_json(f.read())
    env = DEFAULT_ENV.copy()
    env.cluster_config = cluster_config
    branch = Branch(full_shard_id)
    shard_db = ShardDbOperator(db, env, branch)

    class CursorShardState:
        def __init__(self):
            self.db = shard_db
            self.branch = branch
            self.full_shard_id = full_shard_id
            self.env = env
            self.genesis_token_id = token_id_encode(cluster_config.QUARKCHAIN.GENESIS_TOKEN)

        def _is_neighbor(self, remote_branch, root_height=None):
            shard_size = len(
                self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
                    root_height
                )
            )
            return is_neighbor(self.branch, remote_branch, shard_size)

    cursor_shard_state = CursorShardState()

def same_cursor(a, b):
    return (
        a.root_block_height == b.root_block_height
        and a.minor_block_index == b.minor_block_index
        and a.xshard_deposit_index == b.xshard_deposit_index
    )

def encode_address(address):
    return data_encoder(address.serialize())

def encode_deposit(deposit):
    return {
        "txHash": data_encoder(deposit.tx_hash),
        "fromAddress": encode_address(deposit.from_address),
        "toAddress": encode_address(deposit.to_address),
        "value": hex(deposit.value),
        "gasPrice": hex(deposit.gas_price),
        "gasTokenId": hex(deposit.gas_token_id),
        "transferTokenId": hex(deposit.transfer_token_id),
        "gasRemained": hex(deposit.gas_remained),
        "messageData": data_encoder(deposit.message_data),
        "createContract": deposit.create_contract,
        "isFromRootChain": deposit.is_from_root_chain,
        "refundRate": hex(deposit.refund_rate),
    }

def reconstruct_deposits(block, block_hash, xdraw):
    if block.header.height == 0 or not xdraw or cursor_shard_state is None:
        return []
    target = block.meta.xshard_tx_cursor_info
    cursor = XshardTxCursor(cursor_shard_state, block.header)
    deposits = []
    for _ in range(100000):
        deposit = cursor.get_next_tx()
        if deposit is None:
            break
        deposits.append(deposit)
        if same_cursor(cursor.get_cursor_info(), target):
            break
    if not same_cursor(cursor.get_cursor_info(), target):
        raise SystemExit("failed to reconstruct xshard deposits for block {}".format(block_hash.hex()))
    return deposits

def export_block(height):
    block_hash = db.get(("mi_%d" % height).encode())
    if not block_hash:
        raise SystemExit("minor block not found: full_shard_id={} height={}".format(full_shard_id, height))
    raw_block = db.get(b"mblock_" + block_hash)
    if not raw_block:
        raise SystemExit("minor block body missing: hash={}".format(block_hash.hex()))
    block = MinorBlock.deserialize(raw_block)
    out = minor_block_encoder(block, include_transactions=True, extra_info=None)
    out["hashEvmReceiptRoot"] = data_encoder(block.meta.hash_evm_receipt_root)
    out["crossShardReceiveGasUsed"] = hex(block.meta.evm_cross_shard_receive_gas_used)
    out["xshardTxCursorInfo"] = {
        "rootBlockHeight": hex(block.meta.xshard_tx_cursor_info.root_block_height),
        "minorBlockIndex": hex(block.meta.xshard_tx_cursor_info.minor_block_index),
        "xshardDepositIndex": hex(block.meta.xshard_tx_cursor_info.xshard_deposit_index),
    }
    for i, tx in enumerate(block.tx_list):
        evm_tx = tx.tx.to_evm_tx()
        out["transactions"][i]["rawTypedTransaction"] = data_encoder(tx.serialize())
        out["transactions"][i]["rawEvmRlp"] = data_encoder(rlp.encode(evm_tx))

    xdraw = db.get(b"xd_" + block_hash, None)
    out["xshardReceiveDepositHashes"] = []
    if xdraw:
        for h in HashList.deserialize(xdraw).hlist:
            out["xshardReceiveDepositHashes"].append(data_encoder(h))

    xraw = db.get(b"xr_" + block_hash, None)
    deposits = []
    if xraw:
        deposits = CrossShardTransactionList.from_data(xraw).tx_list
    else:
        deposits = reconstruct_deposits(block, block_hash, xdraw)
    out["xshardReceiveDeposits"] = [encode_deposit(deposit) for deposit in deposits]
    return out

print(json.dumps([export_block(height) for height in range(start_height, end_height + 1)]))
`
