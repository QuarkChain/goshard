package main

import (
	"testing"

	"github.com/ethereum/go-ethereum/qkc/config"
	"github.com/ethereum/go-ethereum/qkc/consensus/doublesha256"
	"github.com/ethereum/go-ethereum/qkc/consensus/simulate"
	"github.com/stretchr/testify/assert"
)

func TestParseShardIDs(t *testing.T) {
	ids, err := parseShardIDs("2,3,0x10002")
	assert.NoError(t, err)
	assert.Equal(t, []uint32{2, 3, 0x10002}, ids)

	// whitespace and a single id
	ids, err = parseShardIDs(" 0x20003 ")
	assert.NoError(t, err)
	assert.Equal(t, []uint32{0x20003}, ids)

	_, err = parseShardIDs("2,2")
	assert.Error(t, err, "duplicate ids should be rejected")
	_, err = parseShardIDs("")
	assert.Error(t, err, "empty list should be rejected")
	_, err = parseShardIDs("zzz")
	assert.Error(t, err, "non-numeric id should be rejected")
}

func TestShardDBPath(t *testing.T) {
	assert.Equal(t, "/data/slave0/shard-0x2", shardDBPath("/data/slave0", 0x2))
	assert.Equal(t, "/data/slave0/shard-0x10002", shardDBPath("/data/slave0", 0x10002))

	assert.Equal(t, "memory", databaseDesc("", 0x2))
	assert.Equal(t, "/d/shard-0x3", databaseDesc("/d", 0x3))
}

func TestNewEngineSelection(t *testing.T) {
	q := config.NewClusterConfig().Quarkchain

	shardCfg := q.GetShardConfigByFullShardID(0x2)
	shardCfg.ConsensusType = config.PoWDoubleSha256
	_, ok := newEngine(shardCfg).(*doublesha256.DoubleSHA256)
	assert.True(t, ok, "POW_DOUBLESHA256 should select the double-sha256 engine")

	shardCfg.ConsensusType = config.PoWSimulate
	_, ok = newEngine(shardCfg).(*simulate.PowSimulate)
	assert.True(t, ok, "POW_SIMULATE should select the simulate engine")
}
