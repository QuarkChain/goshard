// Copyright 2026-2027, QuarkChain.

package core

import "errors"

// Errors from transaction validation, in the order validate_transaction
// (messages.py:134) raises them. The order is the error priority: a
// transaction that breaks several rules is reported against the first.
var (
	ErrUnsignedTransaction  = errors.New("qkc: transaction is unsigned")
	ErrInvalidTransaction   = errors.New("qkc: invalid transaction")
	ErrInvalidNonce         = errors.New("qkc: invalid nonce")
	ErrInsufficientStartGas = errors.New("qkc: start gas below intrinsic gas")
	ErrInsufficientBalance  = errors.New("qkc: insufficient balance")
	ErrInvalidNativeToken   = errors.New("qkc: invalid native token")
	ErrBlockGasLimitReached = errors.New("qkc: block gas limit reached")
)

// ErrUnsupportedNativeToken marks a boundary of the execution profile: an
// account holding more non-zero token balances than the account leaf's list
// encoding takes, where pyquarkchain switches those balances to a secure trie
// (state.py:100-135). It is not a QuarkChain consensus outcome — it says the
// state cannot be represented — so it must reach the caller and abandon the
// block rather than become a failed transaction.
var ErrUnsupportedNativeToken = errors.New("qkc: token trie balances are not implemented")

// ErrGasUnderflow is the other boundary, and it points the opposite way: not a
// state this fork cannot write, but a transaction pyquarkchain cannot execute.
// Its gas counter can be driven below zero (see vm.EVM.QKCGasUnderflow), and
// apply_transaction then trips `assert gas_remained >= 0` (messages.py:305).
//
// A transaction like that is absent from mainnet history, because no
// pyquarkchain node can mine or accept a block holding one. Executing it here
// anyway — cleanly, as an out-of-gas — would make this fork the only client
// that accepts such a block. Refusing the whole block is what keeps the two in
// agreement.
var ErrGasUnderflow = errors.New("qkc: pyquarkchain cannot execute this transaction: its gas counter would go below zero")

// ErrMissingRootBlock is chain data the cursor needed and did not get: a root
// block at or below the block's own root tip. It is not an end of stream, and
// must not be mistaken for one — a node that skipped those deposits would
// compute a state root that every node holding the data rejects.
var ErrMissingRootBlock = errors.New("qkc: root block missing below the root tip")

// Errors from block-level validation: the seven results add_block compares
// (shard_state.py:940-994).
var (
	ErrStateRootMismatch      = errors.New("qkc: state root mismatch")
	ErrReceiptRootMismatch    = errors.New("qkc: receipt root mismatch")
	ErrGasUsedMismatch        = errors.New("qkc: gas used mismatch")
	ErrXShardGasUsedMismatch  = errors.New("qkc: cross-shard receive gas used mismatch")
	ErrXShardCursorMismatch   = errors.New("qkc: cross-shard transaction cursor mismatch")
	ErrCoinbaseAmountMismatch = errors.New("qkc: coinbase amount mismatch")
	ErrBloomMismatch          = errors.New("qkc: bloom mismatch")
)
