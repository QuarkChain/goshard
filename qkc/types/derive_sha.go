// Copyright 2026-2027, QuarkChain.

// QKC trie/hash helpers follow pyquarkchain-compatible wire hashing.
// Modified from go-ethereum under GNU Lesser General Public License

package types

import (
	"bytes"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	coretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
)

type DerivableList interface {
	Len() int
	Bytes(i int) []byte
}

func DeriveSha(list DerivableList, hasher coretypes.ListHasher) common.Hash {
	hasher.Reset()
	keybuf := new(bytes.Buffer)
	for i := 0; i < list.Len(); i++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i))
		if err := hasher.Update(keybuf.Bytes(), list.Bytes(i)); err != nil {
			panic(err)
		}
	}
	return hasher.Hash()
}

var EmptyTrieHash = coretypes.EmptyRootHash

var EmptyHash = common.Hash{}

func CalculateMerkleRoot(list interface{}) (h common.Hash) {
	val := reflect.ValueOf(list)
	// Use Kind() rather than Type().Kind(): a nil list is the zero reflect.Value,
	// whose Kind() is Invalid (no panic), so the clear error below fires instead of
	// Type()'s opaque "reflect: call of reflect.Value.Type on zero Value" panic.
	if val.Kind() != reflect.Slice {
		panic("expect slice input for CalculateMerkleRoot")
	}
	hashList := make([]common.Hash, val.Len())
	if val.Len() == 0 {
		hashList = append(hashList, common.Hash{})
	} else {
		for i := 0; i < val.Len(); i++ {
			bytes, err := serialize.SerializeToBytes(val.Index(i).Interface())
			if err != nil {
				// A serialize failure here would silently hash nil and corrupt the
				// Merkle root in consensus-critical paths (NewMinorBlock, Finalize);
				// fail loudly instead, mirroring Receipts.Bytes.
				panic(err)
			}
			hashList[i] = sha3_256(bytes)
		}
	}
	zBytes := common.Hash{}
	for len(hashList) != 1 {
		tempList := make([]common.Hash, 0, (len(hashList)+1)/2)
		if len(hashList)%2 == 1 {
			hashList = append(hashList, zBytes)
		}
		length := len(hashList)
		for i := 0; i < length-1; i = i + 2 {
			tempList = append(tempList,
				sha3_256(append(hashList[i].Bytes(), hashList[i+1].Bytes()...)))

		}
		hashList = tempList
		zBytes = sha3_256(append(zBytes.Bytes(), zBytes.Bytes()...))
	}
	return sha3_256(append(hashList[0].Bytes(), qkcCommon.Uint64ToBytes(uint64(val.Len()))...))
}

func sha3_256(bytes []byte) (hash common.Hash) {
	hw := crypto.NewKeccakState()
	hw.Write(bytes)
	hw.Sum(hash[:0])
	return hash
}
