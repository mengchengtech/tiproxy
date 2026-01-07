// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/policy"
	"github.com/mengchengtech/cerberus/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mockSimpleConn struct {
	sync.Mutex
	t        *testing.T
	kv       map[any]any
	connID   uint64
	backend  BackendInst
	receiver ConnEventReceiver
}

func newMockSimpleConn(t *testing.T, id uint64) *mockSimpleConn {
	return &mockSimpleConn{
		t:      t,
		connID: id,
		kv:     make(map[any]any),
	}
}

func (conn *mockSimpleConn) SetEventReceiver(receiver ConnEventReceiver) {
	conn.Lock()
	conn.receiver = receiver
	conn.Unlock()
}

func (conn *mockSimpleConn) SetValue(k, v any) {
	conn.Lock()
	conn.kv[k] = v
	conn.Unlock()
}

func (conn *mockSimpleConn) Value(k any) any {
	conn.Lock()
	v := conn.kv[k]
	conn.Unlock()
	return v
}

func (conn *mockSimpleConn) ConnectionID() uint64 {
	return conn.connID
}

func (conn *mockSimpleConn) ConnInfo() []zap.Field {
	return nil
}

type mockBackendObserver struct {
	healthLock     sync.Mutex
	healths        map[string]*observer.BackendHealth
	subscriberLock sync.Mutex
	subscribers    map[string]chan observer.HealthResult
}

func newMockBackendObserver() *mockBackendObserver {
	return &mockBackendObserver{
		healths:     make(map[string]*observer.BackendHealth),
		subscribers: make(map[string]chan observer.HealthResult),
	}
}

func (mbo *mockBackendObserver) addBackend(addr string) {
	mbo.healthLock.Lock()
	defer mbo.healthLock.Unlock()
	mbo.healths[addr] = &observer.BackendHealth{
		Healthy: true,
	}
}

func (mbo *mockBackendObserver) Start(ctx context.Context) {
}

func (mbo *mockBackendObserver) Subscribe(name string) <-chan observer.HealthResult {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	subscriber := make(chan observer.HealthResult)
	mbo.subscribers[name] = subscriber
	return subscriber
}

func (mbo *mockBackendObserver) Unsubscribe(name string) {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	if subscriber, ok := mbo.subscribers[name]; ok {
		close(subscriber)
		delete(mbo.subscribers, name)
	}
}

func (mbo *mockBackendObserver) Refresh() {
	mbo.addBackend("0")
}

func (mbo *mockBackendObserver) notify(err error) {
	mbo.healthLock.Lock()
	healths := make(map[string]*observer.BackendHealth, len(mbo.healths))
	for addr, health := range mbo.healths {
		healths[addr] = health
	}
	mbo.healthLock.Unlock()
	mbo.subscriberLock.Lock()
	for _, subscriber := range mbo.subscribers {
		subscriber <- observer.NewHealthResult(healths, err)
	}
	mbo.subscriberLock.Unlock()
}

func (mbo *mockBackendObserver) Close() {
	mbo.subscriberLock.Lock()
	defer mbo.subscriberLock.Unlock()
	for _, subscriber := range mbo.subscribers {
		close(subscriber)
	}
}

var _ policy.BalancePolicy = (*mockBalancePolicy)(nil)

type mockBalancePolicy struct {
	cfg               atomic.Pointer[config.Config]
	backendsToBalance func([]policy.BackendCtx) (from policy.BackendCtx, to policy.BackendCtx, balanceCount float64, reason string, logFields []zapcore.Field)
	backendToRoute    func([]policy.BackendCtx) policy.BackendCtx
}

func (m *mockBalancePolicy) Init(cfg *config.Config) {
	m.cfg.Store(cfg)
}

func (m *mockBalancePolicy) BackendToRoute(backends []policy.BackendCtx) policy.BackendCtx {
	if m.backendToRoute != nil {
		return m.backendToRoute(backends)
	}
	return nil
}

func (m *mockBalancePolicy) BackendsToBalance(backends []policy.BackendCtx) (from policy.BackendCtx, to policy.BackendCtx, balanceCount float64, reason string, logFields []zapcore.Field) {
	if m.backendsToBalance != nil {
		return m.backendsToBalance(backends)
	}
	return nil, nil, 0, "", nil
}

func (m *mockBalancePolicy) SetConfig(cfg *config.Config) {
	m.cfg.Store(cfg)
}
