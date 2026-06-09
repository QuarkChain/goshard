// Ported from github.com/QuarkChain/goquarkchain/core/types (byte-compatible).
// Modified from go-ethereum under GNU Lesser General Public License
//
// Adaptations (hash output identical):
//   - new(trie.Trie)/trie.Update -> trie.NewEmpty(...)/MustUpdate (modern trie API
//     requires a backing node database; an ephemeral in-memory one is used).
//   - sha3.NewKeccak256() -> crypto.NewKeccakState().

package types

import (
	"bytes"
	"reflect"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	qkcCommon "github.com/ethereum/go-ethereum/qkc/common"
	"github.com/ethereum/go-ethereum/qkc/serialize"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

type DerivableList interface {
	Len() int
	Bytes(i int) []byte
}

func DeriveSha(list DerivableList) common.Hash {
	keybuf := new(bytes.Buffer)
	trie := trie.NewEmpty(triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil))
	for i := 0; i < list.Len(); i++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i))
		trie.MustUpdate(keybuf.Bytes(), list.Bytes(i))
	}
	return trie.Hash()
}

var EmptyTrieHash = trie.NewEmpty(triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil)).Hash()

var EmptyHash = common.Hash{}

func CalculateMerkleRoot(list interface{}) (h common.Hash) {
	val := reflect.ValueOf(list)
	if val.Type().Kind() != reflect.Slice {
		panic("expect slice input for CalculateMerkleRoot")
	}
	hashList := make([]common.Hash, val.Len())
	if val.Len() == 0 {
		hashList = append(hashList, common.Hash{})
	} else {
		for i := 0; i < val.Len(); i++ {
			bytes, _ := serialize.SerializeToBytes(val.Index(i).Interface())
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
