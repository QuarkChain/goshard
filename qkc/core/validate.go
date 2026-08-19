// Copyright 2026-2027, QuarkChain.

package core

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/qkc/account"
	qkccommon "github.com/ethereum/go-ethereum/qkc/common"
	qkcstate "github.com/ethereum/go-ethereum/qkc/state"
	"github.com/ethereum/go-ethereum/qkc/types"
)

// maxFutureTxNonce is MAX_FUTURE_TX_NONCE (shard_state.py:55): how far ahead of
// the account's nonce a transaction may sit and still be accepted into a block.
const maxFutureTxNonce = 64

// uint128Max bounds start gas and gas price (messages.py:186).
var uint128Max = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

// IntrinsicGas is Transaction.intrinsic_gas_used (transactions.py:213). Note the
// cross-shard surcharge: a cross-shard transaction pays for the deposit up
// front, on the source shard.
func IntrinsicGas(tx *types.Transaction) uint64 {
	data := tx.Data()
	var zeros uint64
	for _, b := range data {
		if b == 0 {
			zeros++
		}
	}
	nonZeros := uint64(len(data)) - zeros

	gas := GTXCost
	if tx.To() == nil {
		gas += CreateContractGas
	}
	gas += GTXDataZero*zeros + GTXDataNonZero*nonZeros
	if tx.IsCrossShard() {
		gas += GTXXShardCost
	}
	return gas
}

// TxSender recovers the sender for the signing rules of tx's version.
//
// Two of the version 2 rules are enforced during recovery here but belong to
// validate_transaction upstream (messages.py:160-176), where they are ordinary
// invalid transactions rather than unsigned ones. The rejection is the same
// either way; reporting them under the rule pyquarkchain names keeps the error
// priority readable and lets a vector pin which rule refused a transaction.
func TxSender(ctx *ExecutionContext, tx *types.Transaction) (account.Recipient, error) {
	signer := types.MakeSigner(ctx.QKCConfig.NetworkID, ctx.ShardConfig.EthChainID)
	sender, err := types.Sender(signer, tx)
	if err != nil {
		if errors.Is(err, types.ErrV2NonDefaultToken) || errors.Is(err, types.ErrV2CrossShard) {
			return account.Recipient{}, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
		}
		return account.Recipient{}, fmt.Errorf("%w: %v", ErrUnsignedTransaction, err)
	}
	return sender, nil
}

// ValidateTransaction is validate_transaction (messages.py:134), in the source's
// order, because the order is the error priority.
func ValidateTransaction(ctx *ExecutionContext, state *qkcstate.EvmState, tx *types.Transaction, sender account.Recipient) error {
	// (1) the signature is valid — the caller recovered the sender, so reaching
	// here with a zero address means recovery was skipped, not that it passed.

	if tx.Version() == 2 {
		if err := validateEIP155(ctx, state, tx); err != nil {
			return err
		}
	}

	// (1a) start gas, gas price and both token ids are bounded.
	if new(big.Int).SetUint64(tx.Gas()).Cmp(uint128Max) > 0 ||
		tx.GasPrice().Cmp(uint128Max) > 0 ||
		tx.GasTokenID() > qkccommon.TOKENIDMAX ||
		tx.TransferTokenID() > qkccommon.TOKENIDMAX {
		return fmt.Errorf("%w: start gas, gas price and token ids must fit in 128 bits", ErrInvalidTransaction)
	}

	// (2) the nonce is the account's current one.
	if reqNonce := state.GetNonce(sender); reqNonce != tx.Nonce() {
		return fmt.Errorf("%w: nonce %d, want %d", ErrInvalidNonce, tx.Nonce(), reqNonce)
	}

	// (3) the gas limit covers the intrinsic gas.
	intrinsic := IntrinsicGas(tx)
	if tx.Gas() < intrinsic {
		return fmt.Errorf("%w: start gas %d, intrinsic %d", ErrInsufficientStartGas, tx.Gas(), intrinsic)
	}

	// (4) a non-default token the sender does not hold at all is refused before
	// any amount is compared. Holding zero of a token and holding none of it are
	// the same balance, but not the same transaction: the chain's own token is
	// spendable from empty, a foreign one is not.
	defaultToken := state.DefaultChainTokenID()
	balances := map[uint64]*big.Int{
		tx.TransferTokenID(): state.GetBalance(sender, tx.TransferTokenID()).ToBig(),
		tx.GasTokenID():      state.GetBalance(sender, tx.GasTokenID()).ToBig(),
	}
	for _, tokenID := range [...]uint64{tx.TransferTokenID(), tx.GasTokenID()} {
		if tokenID != defaultToken && balances[tokenID].Sign() == 0 {
			return fmt.Errorf("%w: non-default token %d has zero balance",
				ErrInvalidNativeToken, tokenID)
		}
	}

	// (5) the balance covers value plus the whole gas allowance, per token: the
	// two costs add only when the transfer and gas tokens are the same one.
	costs := map[uint64]*big.Int{
		tx.TransferTokenID(): new(big.Int),
		tx.GasTokenID():      new(big.Int),
	}
	costs[tx.TransferTokenID()].Add(costs[tx.TransferTokenID()], tx.Value())
	costs[tx.GasTokenID()].Add(costs[tx.GasTokenID()],
		new(big.Int).Mul(tx.GasPrice(), new(big.Int).SetUint64(tx.Gas())))
	for tokenID, cost := range costs {
		if balances[tokenID].Cmp(cost) < 0 {
			return fmt.Errorf("%w: token %d, have %s, need %s",
				ErrInsufficientBalance, tokenID, balances[tokenID], cost)
		}
	}

	// (6) a foreign gas token has to be priced by the general native token
	// manager, and the manager has to hold enough genesis token to settle the
	// whole allowance at that price. The quote is taken and unwound: paying for
	// it here would charge the transaction twice.
	if tx.GasPrice().Sign() != 0 && tx.GasTokenID() != defaultToken {
		_, converted, err := quoteNativeTokenGas(ctx, state, tx.GasTokenID(), tx.Gas(), tx.GasPrice())
		if err != nil {
			return err
		}
		if converted.Sign() == 0 {
			return fmt.Errorf("%w: non-default gas token %d cannot be used to pay gas",
				ErrInvalidNativeToken, tx.GasTokenID())
		}
		reserve := state.GetBalance(generalNativeTokenContractAddress, state.GenesisTokenID()).ToBig()
		if need := new(big.Int).Mul(converted, new(big.Int).SetUint64(tx.Gas())); reserve.Cmp(need) < 0 {
			return fmt.Errorf("%w: gas reserve %s does not cover %s for token %d",
				ErrInvalidNativeToken, reserve, need, tx.GasTokenID())
		}
	}

	// (7) the block has room for the whole gas allowance, not just what the
	// transaction will use.
	if state.GasUsed()+tx.Gas() > state.GasLimit() {
		return fmt.Errorf("%w: %d + %d > %d", ErrBlockGasLimitReached, state.GasUsed(), tx.Gas(), state.GasLimit())
	}
	return nil
}

// validateEIP155 is the version 2 prelude (messages.py:139-183). Mainnet only
// enables it in September 2021, well past the supported window; devnet enables
// it at genesis, so it is exercised from the first block there.
func validateEIP155(ctx *ExecutionContext, state *qkcstate.EvmState, tx *types.Transaction) error {
	chainConfig, ok := ctx.QKCConfig.Chains[tx.FromChainID()]
	if !ok {
		return fmt.Errorf("%w: no chain %d", ErrInvalidTransaction, tx.FromChainID())
	}
	defaultToken, err := qkccommon.TokenIDEncodeChecked(chainConfig.DefaultChainToken)
	if err != nil {
		return fmt.Errorf("%w: chain %d default token: %v", ErrInvalidTransaction, tx.FromChainID(), err)
	}
	if state.Timestamp() < ctx.QKCConfig.EnableEIP155SignerTimestamp {
		return fmt.Errorf("%w: EIP155 signer is not enabled yet", ErrInvalidTransaction)
	}
	v, _, _ := tx.RawSignatureValues()
	base := new(big.Int).Mul(big.NewInt(2), new(big.Int).SetUint64(uint64(tx.NetworkId())))
	if v.Cmp(new(big.Int).Add(base, big.NewInt(35))) != 0 && v.Cmp(new(big.Int).Add(base, big.NewInt(36))) != 0 {
		return fmt.Errorf("%w: network id %d does not match signature v %s", ErrInvalidTransaction, tx.NetworkId(), v)
	}
	if tx.FromChainID() != tx.ToChainID() || tx.FromShardKey() != 0 || tx.ToShardKey() != 0 {
		return fmt.Errorf("%w: EIP155 signer does not support cross-shard transactions", ErrInvalidTransaction)
	}
	if tx.GasTokenID() != defaultToken || tx.TransferTokenID() != defaultToken {
		return fmt.Errorf("%w: EIP155 signer only supports %s", ErrInvalidTransaction, chainConfig.DefaultChainToken)
	}
	if tx.NetworkId() != chainConfig.EthChainID {
		return fmt.Errorf("%w: network id %d is not the chain's eth chain id %d", ErrInvalidTransaction, tx.NetworkId(), chainConfig.EthChainID)
	}
	// eth_chain_id is network_id (transactions.py:279), and the chain ids are
	// laid out contiguously above BASE_ETH_CHAIN_ID.
	if uint64(tx.NetworkId()) != uint64(ctx.QKCConfig.BaseEthChainID)+uint64(tx.FromChainID())+1 {
		return fmt.Errorf("%w: eth chain id %d does not encode chain %d", ErrInvalidTransaction, tx.NetworkId(), tx.FromChainID())
	}
	return nil
}

// ValidateTxForBlock is __validate_tx (shard_state.py:420): the shard-level
// checks a transaction must pass to be in a block, on top of validate_transaction.
//
// The two are not simply run in sequence. A transaction whose nonce is ahead of
// the account's but within the future-nonce window returns here without running
// validate_transaction at all (shard_state.py:520) — so its balance and gas are
// never checked, and apply_transaction is left to reject it. Collapsing the two
// into one pass would accept different transactions.
func ValidateTxForBlock(ctx *ExecutionContext, state *qkcstate.EvmState, tx *types.Transaction, xShardGasLimit uint64) (account.Recipient, error) {
	sender, err := TxSender(ctx, tx)
	if err != nil {
		return account.Recipient{}, err
	}

	if tx.Version() == 2 {
		// The middle layer rewrites network_id to the eth chain id for v2.
		if tx.NetworkId() != ctx.ShardConfig.EthChainID {
			return sender, fmt.Errorf("%w: network id %d, want eth chain id %d",
				ErrInvalidTransaction, tx.NetworkId(), ctx.ShardConfig.EthChainID)
		}
	} else if tx.NetworkId() != ctx.QKCConfig.NetworkID {
		return sender, fmt.Errorf("%w: network id %d, want %d",
			ErrInvalidTransaction, tx.NetworkId(), ctx.QKCConfig.NetworkID)
	}

	branch := ctx.Branch()
	if !branch.IsInBranch(tx.FromFullShardKey()) {
		return sender, fmt.Errorf("%w: from full shard key %#x is not in branch %#x",
			ErrInvalidTransaction, tx.FromFullShardKey(), branch.GetFullShardID())
	}

	if tx.IsCrossShard() {
		toBranch := account.NewBranch(tx.ToFullShardId())
		rootHeight := uint64(0)
		if ctx.ValidationRootTip != nil {
			rootHeight = uint64(ctx.ValidationRootTip.Number)
		}
		if !ctx.shardInitializedBefore(toBranch.GetFullShardID(), rootHeight) {
			return sender, fmt.Errorf("%w: target shard %#x is not initialized at root height %d",
				ErrInvalidTransaction, toBranch.GetFullShardID(), rootHeight)
		}
		if !ctx.isNeighbor(toBranch, rootHeight) {
			return sender, fmt.Errorf("%w: target shard %#x is not a neighbour of %#x",
				ErrInvalidTransaction, toBranch.GetFullShardID(), branch.GetFullShardID())
		}
		if tx.Gas() > xShardGasLimit {
			return sender, fmt.Errorf("%w: cross-shard start gas %d exceeds the cross-shard limit %d",
				ErrInvalidTransaction, tx.Gas(), xShardGasLimit)
		}
	}

	// Before transactions were switched on, only whitelisted senders could send
	// any (shard_state.py:501).
	if state.Timestamp() < ctx.QKCConfig.EnableTxTimeStamp &&
		!ctx.QKCConfig.IsTxSenderWhitelisted(sender) {
		return sender, fmt.Errorf("%w: sender %s is not whitelisted before transactions are enabled",
			ErrInvalidTransaction, sender.Hex())
	}

	// Before the EVM was switched on, no transaction could deploy or call code.
	if state.Timestamp() < ctx.QKCConfig.EnableEvmTimeStamp {
		if tx.To() == nil || len(tx.Data()) != 0 {
			return sender, fmt.Errorf("%w: contract transactions are not allowed before the EVM is enabled",
				ErrInvalidTransaction)
		}
	}

	reqNonce := state.GetNonce(sender)
	if reqNonce < tx.Nonce() && tx.Nonce() <= reqNonce+maxFutureTxNonce {
		return sender, nil
	}
	return sender, ValidateTransaction(ctx, state, tx, sender)
}

// shardInitializedBefore is get_initialized_full_shard_ids_before_root_height
// (config.py:449) reduced to the membership test the caller wants.
func (ctx *ExecutionContext) shardInitializedBefore(fullShardID uint32, rootHeight uint64) bool {
	shard := ctx.QKCConfig.GetShardConfigByFullShardID(fullShardID)
	if shard == nil || shard.Genesis == nil {
		return false
	}
	return uint64(shard.Genesis.RootHeight) < rootHeight
}

// initializedShardCount is the shard size the neighbour test uses: how many
// shards exist as of the given root height, not how many the config declares.
func (ctx *ExecutionContext) initializedShardCount(rootHeight uint64) uint32 {
	var count uint32
	for _, id := range ctx.QKCConfig.GetGenesisShardIds() {
		if ctx.shardInitializedBefore(id, rootHeight) {
			count++
		}
	}
	return count
}

// isNeighbor is is_neighbor (neighbor.py:5). At the shard counts QuarkChain
// actually runs it is always true, which is why the mainnet window has no
// non-neighbour rejections to replay; the real branches only matter above 32
// shards.
func (ctx *ExecutionContext) isNeighbor(remote account.Branch, rootHeight uint64) bool {
	return IsNeighbor(ctx.Branch(), remote, ctx.initializedShardCount(rootHeight))
}

// IsNeighbor decides whether two branches may exchange cross-shard
// transactions directly.
func IsNeighbor(b1, b2 account.Branch, shardSize uint32) bool {
	if shardSize <= 32 {
		return true
	}
	if b1.GetChainID() == b2.GetChainID() {
		return isPowerOfTwo(absDiff(b1.GetShardID(), b2.GetShardID()))
	}
	if b1.GetShardID() == b2.GetShardID() {
		return isPowerOfTwo(absDiff(b1.GetChainID(), b2.GetChainID()))
	}
	return false
}

func absDiff(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}

func isPowerOfTwo(v uint32) bool {
	return v != 0 && v&(v-1) == 0
}
