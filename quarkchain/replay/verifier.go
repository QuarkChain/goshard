// Copyright 2026-2027, QuarkChain.

package replay

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	qkcstate "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/triedb"
)

type Verifier struct {
	config            *qkcstate.QuarkChainClusterGenesisConfig
	fullShardID       uint32
	stateFullShardKey uint32
	accounts          map[common.Address]qkcstate.QuarkChainAccount
}

type BlockResult struct {
	FullShardID       uint32
	Height            uint64
	BlockHash         common.Hash
	ExpectedStateRoot common.Hash
	GotStateRoot      common.Hash
}

type MismatchError struct {
	Result BlockResult
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("qkc replay state root mismatch fullShard=%#x height=%d block=%s expected=%s got=%s",
		e.Result.FullShardID, e.Result.Height, e.Result.BlockHash, e.Result.ExpectedStateRoot, e.Result.GotStateRoot)
}

type UnsupportedBlockError struct {
	FullShardID              uint32
	Height                   uint64
	BlockHash                common.Hash
	TransactionCount         int
	XShardReceiveDepositCnt  int
	XShardReceiveDepositHash int
	Reason                   string
}

func (e *UnsupportedBlockError) Error() string {
	reason := e.Reason
	if reason == "" {
		reason = "unsupported execution input"
	}
	return fmt.Sprintf("qkc replay execution unsupported fullShard=%#x height=%d block=%s txs=%d xshardDeposits=%d xshardDepositHashes=%d reason=%s",
		e.FullShardID, e.Height, e.BlockHash, e.TransactionCount, e.XShardReceiveDepositCnt, e.XShardReceiveDepositHash, reason)
}

func NewVerifier(config *qkcstate.QuarkChainClusterGenesisConfig, fullShardKey uint32) (*Verifier, error) {
	fullShardID, err := FullShardIDFromKey(config, fullShardKey)
	if err != nil {
		return nil, err
	}
	accountsByShard, err := config.GenesisAccountsByFullShardID()
	if err != nil {
		return nil, err
	}
	accounts := cloneAccounts(accountsByShard[fullShardID])
	if accounts == nil {
		accounts = make(map[common.Address]qkcstate.QuarkChainAccount)
	}
	return &Verifier{config: config, fullShardID: fullShardID, accounts: accounts}, nil
}

func FullShardIDFromKey(config *qkcstate.QuarkChainClusterGenesisConfig, fullShardKey uint32) (uint32, error) {
	if config == nil {
		return 0, errors.New("nil qkc cluster config")
	}
	chainID := fullShardKey >> 16
	for _, chain := range config.QuarkChain.Chains {
		if chain.ChainID != chainID {
			continue
		}
		if chain.ShardSize == 0 || chain.ShardSize&(chain.ShardSize-1) != 0 {
			return 0, fmt.Errorf("invalid shard size %d for chain %d", chain.ShardSize, chainID)
		}
		shardID := fullShardKey & (chain.ShardSize - 1)
		return chainID<<16 | chain.ShardSize | shardID, nil
	}
	return 0, fmt.Errorf("chain %d not found in cluster config", chainID)
}

func (v *Verifier) FullShardID() uint32 {
	return v.fullShardID
}

func (v *Verifier) CurrentRoot(db *triedb.Database) (common.Hash, error) {
	return qkcstate.BuildQuarkChainStateRoot(v.accounts, db)
}

func (v *Verifier) VerifyBlock(block *MinorBlockInput) (*BlockResult, error) {
	if block.FullShardID != v.fullShardID {
		return nil, fmt.Errorf("block fullShardId mismatch: verifier %#x block %#x", v.fullShardID, block.FullShardID)
	}
	if block.Height > 0 {
		v.stateFullShardKey = 0
		for idx := range block.Transactions {
			tx := &block.Transactions[idx]
			if tx.EVMTransaction == nil {
				continue
			}
			if err := ValidateHistoricalTransaction(v.config, block, tx); err != nil {
				return nil, fmt.Errorf("validate transaction %d: %w", idx, err)
			}
		}
		for _, deposit := range block.XShardReceiveDeposits {
			if err := v.applyXShardDeposit(deposit); err != nil {
				return nil, &UnsupportedBlockError{
					FullShardID:              block.FullShardID,
					Height:                   block.Height,
					BlockHash:                block.Hash,
					TransactionCount:         len(block.Transactions),
					XShardReceiveDepositCnt:  len(block.XShardReceiveDeposits),
					XShardReceiveDepositHash: block.XShardReceiveDepositHashCnt,
					Reason:                   err.Error(),
				}
			}
		}
		gasUsed, feeTokens, err := v.applyTransactions(block)
		if err != nil {
			return nil, &UnsupportedBlockError{
				FullShardID:              block.FullShardID,
				Height:                   block.Height,
				BlockHash:                block.Hash,
				TransactionCount:         len(block.Transactions),
				XShardReceiveDepositCnt:  len(block.XShardReceiveDeposits),
				XShardReceiveDepositHash: block.XShardReceiveDepositHashCnt,
				Reason:                   err.Error(),
			}
		}
		if len(block.Transactions) != 0 && block.GasUsed != gasUsed {
			return nil, fmt.Errorf("block gasUsed mismatch: got %d want %d", gasUsed, block.GasUsed)
		}
		coinbaseBalances, err := subtractTokenAmounts(block.CoinbaseAmountMap, feeTokens)
		if err != nil {
			return nil, err
		}
		if err := v.applyCoinbase(block.Coinbase, coinbaseBalances); err != nil {
			return nil, err
		}
	}
	root, err := v.CurrentRoot(qkcstate.NewQuarkChainMemoryTrieDB())
	if err != nil {
		return nil, err
	}
	result := &BlockResult{
		FullShardID:       block.FullShardID,
		Height:            block.Height,
		BlockHash:         block.Hash,
		ExpectedStateRoot: block.ExpectedStateRoot,
		GotStateRoot:      root,
	}
	if root != block.ExpectedStateRoot {
		return result, &MismatchError{Result: *result}
	}
	return result, nil
}

func (v *Verifier) applyXShardDeposit(deposit XShardDepositInput) error {
	if !deposit.IsFromRootChain {
		return errors.New("minor-block x-shard deposit execution is not implemented yet")
	}
	if deposit.CreateContract {
		return errors.New("x-shard contract creation is not implemented yet")
	}
	if len(deposit.MessageData) != 0 {
		return errors.New("x-shard message data execution is not implemented yet")
	}
	if deposit.GasRemained != 0 {
		return errors.New("x-shard EVM gas execution is not implemented yet")
	}
	if deposit.GasPrice != nil && deposit.GasPrice.Sign() != 0 {
		return errors.New("x-shard fee execution is not implemented yet")
	}
	return v.creditToken(deposit.ToAddress, deposit.TransferTokenID, deposit.Value)
}

func (v *Verifier) applyCoinbase(coinbase QKCAddress, balances []TokenBalance) error {
	coinbase.FullShardKey = v.stateFullShardKey
	for _, balance := range balances {
		if err := v.creditToken(coinbase, balance.TokenID, balance.Balance); err != nil {
			return err
		}
	}
	return nil
}

func (v *Verifier) creditToken(to QKCAddress, tokenID uint64, amount *big.Int) error {
	if amount == nil || amount.Sign() == 0 {
		return nil
	}
	account, exists := v.accounts[to.Recipient]
	if account.TokenBalances == nil {
		account.TokenBalances = make(map[uint64]*big.Int)
	}
	if !exists {
		account.FullShardKey = to.FullShardKey
	}
	if err := account.AddQuarkChainTokenBalance(tokenID, amount); err != nil {
		return err
	}
	v.accounts[to.Recipient] = account
	return nil
}

func cloneAccounts(input map[common.Address]qkcstate.QuarkChainAccount) map[common.Address]qkcstate.QuarkChainAccount {
	if input == nil {
		return nil
	}
	output := make(map[common.Address]qkcstate.QuarkChainAccount, len(input))
	for addr, account := range input {
		clone := account
		clone.CodeHash = append([]byte(nil), account.CodeHash...)
		clone.Code = append([]byte(nil), account.Code...)
		clone.Optional = append([]byte(nil), account.Optional...)
		if account.TokenBalances != nil {
			clone.TokenBalances = make(map[uint64]*big.Int, len(account.TokenBalances))
			for tokenID, balance := range account.TokenBalances {
				if balance != nil {
					clone.TokenBalances[tokenID] = new(big.Int).Set(balance)
				}
			}
		}
		if account.Storage != nil {
			clone.Storage = make(map[common.Hash]common.Hash, len(account.Storage))
			for key, value := range account.Storage {
				clone.Storage[key] = value
			}
		}
		output[addr] = clone
	}
	return output
}
