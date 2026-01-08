// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"strconv"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/policy"
	"go.uber.org/zap"
)

var _ policy.BackendCtx = (*mockBackend)(nil)

type mockBackend struct {
	observer.BackendInfo
	addr      string
	connScore int
	connCount int
	healthy   bool
	local     bool
}

func newMockBackend(healthy bool, connScore int) *mockBackend {
	return &mockBackend{
		healthy:   healthy,
		connScore: connScore,
		connCount: connScore,
	}
}

func (mb *mockBackend) Healthy() bool {
	return mb.healthy
}

func (mb *mockBackend) ConnScore() int {
	return mb.connScore
}

func (mb *mockBackend) Addr() string {
	return mb.addr
}

func (mb *mockBackend) ConnCount() int {
	return mb.connCount
}

func (mb *mockBackend) GetBackendInfo() observer.BackendInfo {
	return mb.BackendInfo
}

func (mb *mockBackend) Local() bool {
	return mb.local
}

var _ Factor = (*mockFactor)(nil)

type mockFactor struct {
	scores       map[string]int
	bitNum       int
	threshold    int
	balanceCount float64
	updateScore  func(backends []scoredBackend)
	cfg          *config.Config
	advice       BalanceAdvice
	canBeRouted  bool
}

func (mf *mockFactor) Name() string {
	return "mock"
}

func (mf *mockFactor) UpdateScore(backends []scoredBackend) {
	mf.updateScore(backends)
	if mf.scores == nil {
		mf.scores = make(map[string]int)
	}
	for _, backend := range backends {
		mf.scores[backend.Addr()] = backend.factorScore(mf.bitNum)
	}
}

func (mf *mockFactor) ScoreBitNum() int {
	return mf.bitNum
}

func (mf *mockFactor) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	fromScore, toScore := mf.scores[from.Addr()], mf.scores[to.Addr()]
	if mf.advice == AdviceNegtive {
		return AdviceNegtive, 0, nil
	}
	if fromScore-toScore > mf.threshold {
		return AdvicePositive, mf.balanceCount, nil
	}
	return AdviceNeutral, 0, nil
}

func (mf *mockFactor) SetConfig(cfg *config.Config) {
	mf.cfg = cfg
}

func (mf *mockFactor) CanBeRouted(score uint64) bool {
	if mf.canBeRouted {
		return true
	}
	return score == 0
}

func (mf *mockFactor) Close() {
}

func createBackend(backendIdx, connCount, connScore int) scoredBackend {
	host := strconv.Itoa(backendIdx)
	return scoredBackend{
		BackendCtx: &mockBackend{
			BackendInfo: observer.BackendInfo{
				IP:         host,
				StatusPort: 10080,
			},
			addr:      host + ":4000",
			connCount: connCount,
			connScore: connScore,
			healthy:   true,
		},
	}
}
