// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"sort"

	"github.com/mengchengtech/cerberus/pkg/config"
)

var _ BalancePolicy = (*SimpleBalancePolicy)(nil)

// SimpleBalancePolicy is used for serverless tier and testing of router.
// It simply balances by health and connection count.
type SimpleBalancePolicy struct {
}

func NewSimpleBalancePolicy() *SimpleBalancePolicy {
	return &SimpleBalancePolicy{}
}

func (sbp *SimpleBalancePolicy) Init(cfg *config.Config) {
}

func (sbp *SimpleBalancePolicy) BackendToRoute(backends []BackendCtx) BackendCtx {
	if len(backends) == 0 {
		return nil
	}
	sortBackends(backends)
	if backends[0].Healthy() {
		return backends[0]
	}
	return nil
}

func sortBackends(backends []BackendCtx) {
	sort.Slice(backends, func(i, j int) bool {
		if backends[i].Healthy() && !backends[j].Healthy() {
			return true
		}
		return backends[i].Healthy() == backends[j].Healthy() && backends[i].ConnScore() < backends[j].ConnScore()
	})
}
