# singularity

The real QuarkChain singularity cluster configs the goshard slave must consume —
the live **mainnet** and the running **devnet**. These are irreplaceable network
configs, so they live here as shipped artifacts rather than under `testdata/`.

Both: 8 chains (one shard each), 4 slaves `S0..S3` each owning 2 shards,
`FULL_SHARD_ID_LIST` form (S0's first id is written `0x1`), genesis `ALLOC` present.

## `mainnet.json`

`NETWORK_ID 1`, mixed `POW_ETHASH` / `POW_QKCHASH` consensus, root difficulty 1e13.
Copied verbatim from pyquarkchain's
[`mainnet/singularity/cluster_config_template.json`](https://github.com/QuarkChain/pyquarkchain/blob/master/mainnet/singularity/cluster_config_template.json).

## `devnet.json`

`NETWORK_ID 255`, all `POW_SIMULATE`, root difficulty 100000. Copied verbatim from
pyquarkchain's
[`devnet/singularity/cluster_config.json`](https://github.com/QuarkChain/pyquarkchain/blob/master/devnet/singularity/cluster_config.json).

## Pinned root-genesis values

The root genesis header derived from each config (`qkc.CreateRootBlock`) must hash
byte-identically to pyquarkchain's
`GenesisManager.create_root_block().header.get_hash()`. Regenerate with (swap the
path for devnet):

```
# from the root of a pyquarkchain checkout, inside a virtualenv with its
# requirements installed (bare system python lacks e.g. aiohttp):
python -c "
import json
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.genesis import GenesisManager
raw = json.load(open('mainnet/singularity/cluster_config_template.json'))
h = GenesisManager(ClusterConfig.from_dict(raw).QUARKCHAIN).create_root_block().header
print('hash     ', h.get_hash().hex())
print('seal_hash', h.get_hash_for_mining().hex())
print('serialize', h.serialize().hex())
"
```

Current pinned values:

```
mainnet  hash 4036783e441eb5057bf2be96bf1fd4585ac49824de15c0d92a4c14a97886ca51
         seal e7dcdecc09e724ad81e493d70dedcd6d9ea0ee830d7ab2528a5648f2a0cf8178
devnet   hash 5ad443efb7cf5246a3d1bbc1734bd02bf3a5d83bedeccfcfe707d0ebee03780d
         seal 055a7b410a50c098c52c123983e3596c5914ba227bf9e2c0c93309ff8f650d41
```

## Pinned minor-genesis values

`qkc.CreateMinorBlock` must reproduce pyquarkchain's
`GenesisManager.create_minor_block()` exactly — including the genesis state root,
which is what proves the QuarkChain account encoding is byte-compatible. The
pinned output for chain 0's shard (`full_shard_id 0x00000001`) of each network
lives in
[`qkc/testdata/minor_genesis_golden.json`](../../testdata/minor_genesis_golden.json)
and is asserted down to the block hash.

Regenerate with (swap the path for devnet):

```
# from the root of a pyquarkchain checkout, inside a virtualenv with its
# requirements installed:
PYTHONPATH=. python -c "
import json, sys
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.env import Env
from quarkchain.evm.state import State as EvmState
from quarkchain.genesis import GenesisManager

full_shard_id = 0x00000001
cluster = ClusterConfig.from_dict(json.load(open('mainnet/singularity/cluster_config_template.json')))
env = Env(); env.cluster_config = cluster
qkc = cluster.QUARKCHAIN
manager = GenesisManager(qkc)
root = manager.create_root_block()
state = EvmState(env=env.evm_env, db=env.db, qkc_config=qkc)
state.shard_config = qkc.shards[full_shard_id]
block, coinbase = manager.create_minor_block(root, full_shard_id, state)
print('state_root ', block.meta.hash_evm_state_root.hex())
print('meta_hash  ', block.header.hash_meta.hex())
print('header_hash', block.header.get_hash().hex())
print('coinbase   ', block.header.coinbase_address.serialize().hex(), coinbase.balance_map)
print('gas_limit  ', block.header.evm_gas_limit, block.meta.evm_xshard_gas_limit)
"
```

## Execution golden vectors

The execution layer is pinned the same way, but by a script rather than a one-liner:
[`qkc/testdata/gen_exec_golden.py`](../../testdata/gen_exec_golden.py) drives
pyquarkchain's own `EvmState` and writes `qkc/testdata/exec_golden/*.json`.
It reads the two configs in this directory, so the vectors are bound to the
configs goshard ships rather than to whatever a pyquarkchain checkout happens to
carry.

Regenerate with:

```
# from the root of a pyquarkchain checkout, inside a virtualenv with its
# requirements installed:
python <path-to-goshard>/qkc/testdata/gen_exec_golden.py
```

The checkout is taken from `$PYQUARKCHAIN`, defaulting to the current directory.
The script's first two cases are the genesis allocations themselves, and it
fails unless their state roots match the pinned values above — a mismatch
elsewhere is then a real disagreement, not a case description that never reached
`EvmState`.

Consumed by `qkc`, `qkc/config`, `qkc/types`, and `cmd/slave` tests.
