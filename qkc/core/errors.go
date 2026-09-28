// Copyright 2026-2027, QuarkChain.

package core

import "errors"

// Transaction admission errors follow pyquarkchain's validation order.
var (
	ErrUnsignedTransaction  = errors.New("qkc: transaction is unsigned")
	ErrInvalidTransaction   = errors.New("qkc: invalid transaction")
	ErrInvalidNonce         = errors.New("qkc: invalid nonce")
	ErrInsufficientStartGas = errors.New("qkc: start gas below intrinsic gas")
	ErrInsufficientBalance  = errors.New("qkc: insufficient balance")
	ErrBlockGasLimitReached = errors.New("qkc: block gas limit reached")
)
