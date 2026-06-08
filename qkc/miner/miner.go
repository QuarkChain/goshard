// Ported from github.com/QuarkChain/goquarkchain/cluster/miner (faithful).
// Adaptation: the cluster service.ServiceContext dependency (used only for a
// dead-time guard) is dropped — allowMining now gates purely on IsMining.
//
// The Miner orchestrates block production through the consensus engine and is
// agnostic to the algorithm: simulated and real-PoW engines both flow through
// the same commit -> seal -> result loop. GetWork/SubmitWork expose the
// external (RPC) mining path backed by the engine's remote sealer.
package miner

import (
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/qkc/account"
	"github.com/ethereum/go-ethereum/qkc/consensus"
	"github.com/ethereum/go-ethereum/qkc/types"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10
)

var (
	threads = runtime.NumCPU()
)

type workAdjusted struct {
	block              types.IBlock
	adjustedDifficulty *big.Int
	optionalDivider    uint64
}

type Miner struct {
	api    MinerAPI
	engine consensus.Engine

	resultCh chan types.IBlock
	workCh   chan workAdjusted
	startCh  chan struct{}
	exitCh   chan struct{}
	mu       sync.RWMutex
	isMining bool
	stopCh   chan struct{}
	logInfo  string
}

func New(api MinerAPI, engine consensus.Engine) *Miner {
	miner := &Miner{
		api:      api,
		engine:   engine,
		resultCh: make(chan types.IBlock, resultQueueSize),
		workCh:   make(chan workAdjusted, 1),
		startCh:  make(chan struct{}, 1),
		exitCh:   make(chan struct{}),
		stopCh:   make(chan struct{}),
		logInfo:  "miner",
	}
	miner.engine.SetThreads(1)
	go miner.commitLoop()
	go miner.sealLoop()
	go miner.resultLoop()
	return miner
}

func (m *Miner) getTip() uint64 {
	return m.api.GetTip()
}

// interrupt aborts the mining work
func (m *Miner) interrupt() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopCh != nil {
		close(m.stopCh)
		m.stopCh = make(chan struct{})
	}
}

func (m *Miner) allowMining() bool {
	return m.IsMining()
}

func (m *Miner) commit(addr *account.Address) {
	// don't allow to mine
	if m.api.IsSyncing() {
		time.Sleep(500 * time.Millisecond)
		m.startCh <- struct{}{}
		return
	}
	if !m.allowMining() {
		return
	}
	m.interrupt()
	block, diff, optionalDivider, err := m.api.CreateBlockToMine(addr)
	if err != nil {
		log.Error(m.logInfo, "create block to mine err", err)
		// retry to create block to mine
		time.Sleep(2 * time.Second)
		m.startCh <- struct{}{}
		return
	}
	tip := m.getTip()
	if block.NumberU64() <= tip {
		log.Error(m.logInfo, "block's height small than tipHeight after commit blockNumber ,no need to seal", block.NumberU64(), "tip", m.getTip())
		time.Sleep(2 * time.Second)
		m.startCh <- struct{}{}
		return
	}
	m.workCh <- workAdjusted{block, diff, optionalDivider}
}

func (m *Miner) commitLoop() {

	for {
		select {
		case <-m.startCh:
			m.commit(nil)

		case <-m.exitCh:
			log.Debug("commitLoop exit")
			return
		}
	}
}

func (m *Miner) sealLoop() {

	for {
		select {
		case work := <-m.workCh:
			log.Debug(m.logInfo, "ready to seal height", work.block.NumberU64(), "coinbase", work.block.Coinbase().ToHex())
			m.mu.Lock()
			if err := m.engine.Seal(nil, work.block, work.adjustedDifficulty, work.optionalDivider, m.resultCh, m.stopCh); err != nil {
				log.Error(m.logInfo, "Seal block to mine err", err)
				coinbase := work.block.Coinbase()
				m.commit(&coinbase)
			}
			m.mu.Unlock()

		case <-m.exitCh:
			log.Debug("sealLoop exit")
			return
		}
	}
}

func (m *Miner) resultLoop() {

	for {
		select {
		case block := <-m.resultCh:
			log.Debug(m.logInfo, "seal succ number", block.NumberU64(), "hash", block.Hash().String())
			if err := m.api.InsertMinedBlock(block); err != nil {
				log.Error(m.logInfo, "add minered block err block hash", block.Hash().Hex(), "err", err)
				time.Sleep(time.Duration(3) * time.Second)
				coinbase := block.Coinbase()
				m.commit(&coinbase)
			}

		case <-m.exitCh:
			log.Debug("resultLoop exit")
			return
		}
	}
}

func (m *Miner) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isMining && isClosed(m.exitCh) {
		return
	}
	m.isMining = false
	if !isClosed(m.exitCh) {
		close(m.exitCh)
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// SetMining toggles mining on/off; turning it on kicks off the commit loop.
func (m *Miner) SetMining(mining bool) {
	m.mu.Lock()
	m.isMining = mining
	m.mu.Unlock()
	if mining {
		m.startCh <- struct{}{}
	} else {
		m.interrupt()
	}
}

func (m *Miner) GetWork(coinbaseAddr *account.Address) (*consensus.MiningWork, error) {
	addrForGetWork := m.api.GetDefaultCoinbaseAddress()
	if coinbaseAddr != nil && !account.IsSameAddress(*coinbaseAddr, m.api.GetDefaultCoinbaseAddress()) {
		addrForGetWork = *coinbaseAddr
	}

	work, err := m.engine.GetWork(addrForGetWork)
	if err != nil {
		if err == consensus.ErrNoMiningWork {
			block, diff, optionalDivider, err := m.api.CreateBlockToMine(&addrForGetWork)
			if err == nil {
				work := workAdjusted{block, diff, optionalDivider}
				go func() {
					if err := m.engine.Seal(nil, work.block, work.adjustedDifficulty, work.optionalDivider,
						m.resultCh, m.stopCh); err != nil {
						log.Error(m.logInfo, "Seal block to mine err", err)
						coinbase := work.block.Coinbase()
						m.commit(&coinbase)
					}
				}()
				return &consensus.MiningWork{HeaderHash: block.IHeader().SealHash(), Number: block.NumberU64(),
					OptionalDivider: optionalDivider, Difficulty: diff}, nil
			}
			return nil, err
		}
		return nil, err
	}
	return work, nil
}

func (m *Miner) SubmitWork(nonce uint64, hash, digest common.Hash, signature *[65]byte) bool {
	if !m.IsMining() || m.api.IsSyncing() {
		return false
	}
	return m.engine.SubmitWork(nonce, hash, digest, signature)
}

func (m *Miner) HandleNewTip() {
	log.Debug(m.logInfo, "handle new tip: height", m.getTip())
	m.engine.RefreshWork(m.api.GetTip())
	if m.api.IsSyncing() == false {
		m.startCh <- struct{}{}
	}
}

func (m *Miner) IsMining() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isMining
}
