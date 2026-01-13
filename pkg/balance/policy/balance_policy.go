// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package policy

import (
	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
)

type BalancePolicy interface {
	Init(cfg *config.Config)
	BackendToRoute(backends []BackendCtx) BackendCtx
}

type BackendCtx interface {
	Addr() string
	// ConnCount indicates the count of current connections.
	ConnCount() int
	// ConnScore = current connections + incoming connections - outgoing connections.
	ConnScore() int
	Healthy() bool
	GetBackendInfo() observer.BackendInfo
}
