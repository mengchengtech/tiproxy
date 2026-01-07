// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/policy"
	"github.com/mengchengtech/cerberus/pkg/util/errors"
	"github.com/mengchengtech/cerberus/pkg/util/logger"
	"github.com/stretchr/testify/require"
)

type routerTester struct {
	t         *testing.T
	router    *ScoreBasedRouter
	connID    uint64
	backendID int
	backends  map[string]*observer.BackendHealth
	conns     map[uint64]*mockSimpleConn
}

func newRouterTester(t *testing.T, bp policy.BalancePolicy) *routerTester {
	lg, _ := logger.CreateLoggerForTest(t)
	router := NewScoreBasedRouter(lg)
	if bp == nil {
		router.policy = policy.NewSimpleBalancePolicy()
		router.policy.Init(nil)
	} else {
		router.policy = bp
	}
	t.Cleanup(router.Close)
	return &routerTester{
		t:        t,
		router:   router,
		backends: make(map[string]*observer.BackendHealth),
		conns:    make(map[uint64]*mockSimpleConn),
	}
}

func (tester *routerTester) createConn() *mockSimpleConn {
	tester.connID++
	return newMockSimpleConn(tester.t, tester.connID)
}

func (tester *routerTester) notifyHealth() {
	result := observer.NewHealthResult(tester.backends, nil)
	tester.router.updateBackendHealth(result)
}

func (tester *routerTester) addBackends(num int) {
	for i := 0; i < num; i++ {
		tester.backendID++
		addr := strconv.Itoa(tester.backendID)
		tester.backends[addr] = &observer.BackendHealth{
			Healthy: true,
		}
	}
	tester.notifyHealth()
}

func (tester *routerTester) killBackends(num int) {
	killed := 0
	for _, health := range tester.backends {
		if killed >= num {
			break
		}
		if !health.Healthy {
			continue
		}
		health.Healthy = false
		killed++
	}
	tester.notifyHealth()
}

func (tester *routerTester) removeBackends(num int) {
	backends := make([]string, 0, num)
	for addr := range tester.backends {
		if len(backends) >= num {
			break
		}
		backends = append(backends, addr)
	}
	for _, addr := range backends {
		delete(tester.backends, addr)
	}
	tester.notifyHealth()
}

func (tester *routerTester) updateBackendStatusByAddr(addr string, healthy bool) {
	health, ok := tester.backends[addr]
	if ok {
		health.Healthy = healthy
	} else {
		tester.backends[addr] = &observer.BackendHealth{
			Healthy: healthy,
		}
	}
	tester.notifyHealth()
}

func (tester *routerTester) getBackendByIndex(index int) *backendWrapper {
	addr := strconv.Itoa(index + 1)
	backend := tester.router.backends[addr]
	require.NotNil(tester.t, backend)
	return backend
}

func (tester *routerTester) simpleRoute(conn SimpleConn) BackendInst {
	selector := tester.router.GetBackendSelector()
	backend, err := selector.Next()
	if err != ErrNoBackend {
		require.NoError(tester.t, err)
		selector.Finish(conn, true)
	}
	return backend
}

func (tester *routerTester) addConnections(num int) {
	for i := 0; i < num; i++ {
		conn := tester.createConn()
		backend := tester.simpleRoute(conn)
		require.False(tester.t, backend == nil || reflect.ValueOf(backend).IsNil())
		conn.backend = backend
		tester.conns[conn.connID] = conn
	}
}

func (tester *routerTester) checkBackendNum(num int) {
	require.Equal(tester.t, num, len(tester.router.backends))
}

// Test that routing fails when there's no healthy backends.
func TestNoBackends(t *testing.T) {
	tester := newRouterTester(t, nil)
	conn := tester.createConn()
	backend := tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
	require.Equal(t, 0, tester.router.HealthyBackendCount())
	tester.addBackends(1)
	require.Equal(t, 1, tester.router.HealthyBackendCount())
	tester.addConnections(10)
	tester.killBackends(1)
	backend = tester.simpleRoute(conn)
	require.True(t, backend == nil || reflect.ValueOf(backend).IsNil())
	require.Equal(t, 0, tester.router.HealthyBackendCount())
}

// Test that the backends returned by the BackendSelector are complete and different.
func TestSelectorReturnOrder(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	selector := tester.router.GetBackendSelector()
	for i := 0; i < 3; i++ {
		addrs := make(map[string]struct{}, 3)
		for j := 0; j < 3; j++ {
			backend, err := selector.Next()
			require.NoError(t, err)
			addrs[backend.Addr()] = struct{}{}
		}
		// All 3 addresses are different.
		require.Equal(t, 3, len(addrs))
	}

	tester.killBackends(1)
	for i := 0; i < 2; i++ {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err := selector.Next()
	require.NoError(t, err)

	tester.addBackends(1)
	for i := 0; i < 3; i++ {
		_, err := selector.Next()
		require.NoError(t, err)
	}
	_, err = selector.Next()
	require.NoError(t, err)
}

// Test that the backends are balanced even when routing are concurrent.
func TestRouteConcurrently(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	addrs := make(map[string]int, 3)
	selectors := make([]BackendSelector, 0, 30)
	// All the clients are calling Next() but not yet Finish().
	for i := 0; i < 30; i++ {
		selector := tester.router.GetBackendSelector()
		backend, err := selector.Next()
		require.NoError(t, err)
		addrs[backend.Addr()]++
		selectors = append(selectors, selector)
	}
	require.Equal(t, 3, len(addrs))
	for _, num := range addrs {
		require.Equal(t, 10, num)
	}
	for i := 0; i < 3; i++ {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 10, backend.connScore)
	}
	for _, selector := range selectors {
		selector.Finish(nil, false)
	}
	for i := 0; i < 3; i++ {
		backend := tester.getBackendByIndex(i)
		require.Equal(t, 0, backend.connScore)
	}
}

// Test that the backends are refreshed immediately after it's empty.
func TestRefresh(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	rt.Init(context.Background(), bo, policy.NewSimpleBalancePolicy(), nil)
	t.Cleanup(bo.Close)
	t.Cleanup(rt.Close)
	// The initial backends are empty.
	selector := rt.GetBackendSelector()
	_, err := selector.Next()
	require.Equal(t, ErrNoBackend, err)
	// Refresh is called internally and there comes a new one.
	bo.notify(nil)
	require.Eventually(t, func() bool {
		_, err = selector.Next()
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestObserveError(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	rt := NewScoreBasedRouter(lg)
	bo := newMockBackendObserver()
	bo.Start(context.Background())
	rt.Init(context.Background(), bo, policy.NewSimpleBalancePolicy(), nil)
	t.Cleanup(bo.Close)
	t.Cleanup(rt.Close)
	// Mock an observe error.
	bo.notify(errors.New("mock observe error"))
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector()
		_, err := selector.Next()
		return err != nil && err != ErrNoBackend
	}, 3*time.Second, 10*time.Millisecond)
	// Clear the observe error.
	bo.addBackend("0")
	bo.notify(nil)
	require.Eventually(t, func() bool {
		selector := rt.GetBackendSelector()
		_, err := selector.Next()
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSetBackendStatus(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(10)
	tester.killBackends(1)
	for _, conn := range tester.conns {
		require.False(t, conn.backend.Healthy())
	}
	tester.updateBackendStatusByAddr(tester.getBackendByIndex(0).addr, true)
	for _, conn := range tester.conns {
		require.True(t, conn.backend.Healthy())
	}
}

func TestBackendHealthy(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(1)
	tester.addConnections(1)

	conn := tester.conns[1]
	require.True(t, conn.backend.Healthy())
	tester.killBackends(1)
	require.False(t, conn.backend.Healthy())
}

func TestUpdateBackendHealth(t *testing.T) {
	tester := newRouterTester(t, nil)
	tester.addBackends(3)
	// Test some backends are not in the list anymore.
	tester.removeBackends(1)
	tester.checkBackendNum(2)
	// Test some backends failed.
	tester.killBackends(1)
	tester.checkBackendNum(1)
	tester.addBackends(2)
	tester.checkBackendNum(3)
	// The backend won't be removed when there are connections on it.
	tester.addConnections(90)
	tester.killBackends(1)
	tester.checkBackendNum(3)
}
