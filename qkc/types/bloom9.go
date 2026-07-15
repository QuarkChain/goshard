// Copyright 2026-2027, QuarkChain.

// Bloom reuses geth's log-bloom implementation; CreateBloom adapts QKC receipts.

package types

import comtypes "github.com/ethereum/go-ethereum/core/types"

type Bloom = comtypes.Bloom

func CreateBloom(receipts Receipts) Bloom {
	var bloom Bloom
	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}
		for _, log := range receipt.Logs {
			if log == nil {
				continue
			}
			bloom.Add(log.Recipient.Bytes())
			for _, topic := range log.Topics {
				bloom.Add(topic.Bytes())
			}
		}
	}
	return bloom
}
