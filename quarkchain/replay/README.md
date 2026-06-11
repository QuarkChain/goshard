# QuarkChain Replay

`quarkchain/replay` verifies historical PyQuarkChain minor blocks by replaying the
subset of execution that is currently implemented in Go and comparing the
resulting QKC EVM account trie root with `MinorBlockMeta.hash_evm_state_root`.

The package is intentionally separate from normal geth chain import. It treats
the PyQuarkChain RocksDB and cluster config as replay input, then applies state
changes to the QKC account model in `core/state`.

## Flow

1. Load the PyQuarkChain cluster genesis config.
2. Export minor block inputs from a local PyQuarkChain shard DB with `PyDBSource`.
3. Initialize a `Verifier` from the target full shard key and genesis accounts.
4. Replay each block's supported deposits, ordinary transactions, and coinbase
   credits.
5. Commit the QKC account trie and compare the computed root with
   `hashEvmStateRoot`.

The CLI wrapper is `cmd/qkc-replay-verify`:

```sh
go run ./cmd/qkc-replay-verify \
  --pyquarkchain-db /path/to/pyquarkchain/quarkchain/cluster/qkc-data/mainnet \
  --cluster-config quarkchain/mainnet/cluster_config_template.json \
  --full-shard-key 0x00070001 \
  --start 0 \
  --end 199
```

This range includes height `191`, which contains the first supported ordinary
in-shard transfer transaction in the local PyQuarkChain mainnet DB sample.

## Supported Scope

- QKC genesis account loading and state-root comparison.
- PyQuarkChain DB export for minor blocks, transactions, and x-shard receive
  deposits.
- Root-chain/simple x-shard receive deposits without EVM message execution.
- Coinbase token credits.
- Historical QKC transaction envelope parsing, sender recovery, and validation.
- Minimal in-shard ordinary transfer execution for the default QKC token:
  nonce, gas precharge/refund, value transfer, miner fee, and block reward
  accounting.

## Out of Scope

The replay path is not a full historical EVM executor yet. It intentionally
returns unsupported-execution errors for paths that are not implemented,
including contract creation, non-empty transaction data, contract code
execution, cross-shard ordinary transactions, non-default tokens, gas-bearing
x-shard receives, and receipt/meta-root verification.
