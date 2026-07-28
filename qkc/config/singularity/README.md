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

Consumed by `qkc`, `qkc/config`, `qkc/types`, and `cmd/slave` tests.
