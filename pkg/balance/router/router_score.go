// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"
	"net"
	"reflect"
	"sync"

	glist "github.com/bahlo/generic-list-go"
	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/policy"
	"go.uber.org/zap"
)

const (
	_routerKey = "__tiproxy_router"
)

var _ Router = &ScoreBasedRouter{}

// ScoreBasedRouter is an implementation of Router interface.
// It routes a connection based on score.
type ScoreBasedRouter struct {
	sync.Mutex
	logger     *zap.Logger
	policy     policy.BalancePolicy
	observer   observer.BackendObserver
	healthCh   <-chan observer.HealthResult
	cancelFunc context.CancelFunc
	wg         waitgroup.WaitGroup
	// A list of *backendWrapper. The backends are in descending order of scores.
	backends     map[string]*backendWrapper
	observeError error
	// Only store the version of a random backend, so the client may see a wrong version when backends are upgrading.
	serverVersion string
}

// NewScoreBasedRouter creates a ScoreBasedRouter.
func NewScoreBasedRouter(logger *zap.Logger) *ScoreBasedRouter {
	return &ScoreBasedRouter{
		logger:   logger,
		backends: make(map[string]*backendWrapper),
	}
}

func (r *ScoreBasedRouter) Init(ctx context.Context, ob observer.BackendObserver, balancePolicy policy.BalancePolicy, cfg *config.Config) {
	r.observer = ob
	r.healthCh = r.observer.Subscribe("score_based_router")
	r.policy = balancePolicy
	balancePolicy.Init(cfg)
	childCtx, cancelFunc := context.WithCancel(ctx)
	r.cancelFunc = cancelFunc
	// Log the panic.
	r.wg.RunWithRecover(func() {
		r.refreshBackendLoop(childCtx)
	}, nil, r.logger)
}

// GetBackendSelector implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) GetBackendSelector() BackendSelector {
	return BackendSelector{
		routeOnce: router.routeOnce,
		onCreate:  router.onCreateConn,
	}
}

func (router *ScoreBasedRouter) HealthyBackendCount() int {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return 0
	}

	count := 0
	for _, backend := range router.backends {
		if backend.Healthy() {
			count++
		}
	}
	return count
}

func (router *ScoreBasedRouter) getConnEl(conn SimpleConn) *glist.Element[SimpleConn] {
	return conn.Value(_routerKey).(*glist.Element[SimpleConn])
}

func (router *ScoreBasedRouter) setConnEl(conn SimpleConn, ce *glist.Element[SimpleConn]) {
	conn.SetValue(_routerKey, ce)
}

func (router *ScoreBasedRouter) routeOnce(excluded []BackendInst) (BackendInst, error) {
	router.Lock()
	defer router.Unlock()
	if router.observeError != nil {
		return nil, router.observeError
	}

	backends := make([]policy.BackendCtx, 0, len(router.backends))
	for _, backend := range router.backends {
		if !backend.Healthy() {
			continue
		}
		// Exclude the backends that are already tried.
		found := false
		for _, e := range excluded {
			if backend.Addr() == e.Addr() {
				found = true
				break
			}
		}
		if found {
			continue
		}
		backends = append(backends, backend)
	}

	beCtx := router.policy.BackendToRoute(backends)
	if beCtx == nil || reflect.ValueOf(beCtx).IsNil() {
		// No available backends, maybe the health check result is outdated during rolling restart.
		// Refresh the backends asynchronously in this case.
		if router.observer != nil {
			router.observer.Refresh()
		}
		return nil, ErrNoBackend
	}
	backend := beCtx.(*backendWrapper)
	backend.connScore++
	return backend, nil
}

func (router *ScoreBasedRouter) onCreateConn(backendInst BackendInst, conn SimpleConn, succeed bool) {
	router.Lock()
	defer router.Unlock()
	backend := router.ensureBackend(backendInst.Addr())
	if succeed {
		router.addConn(backend, conn)
		conn.SetEventReceiver(router)
	} else {
		backend.connScore--
	}
}

func (router *ScoreBasedRouter) removeConn(backend *backendWrapper, ce *glist.Element[SimpleConn]) {
	backend.connList.Remove(ce)
	router.removeBackendIfEmpty(backend)
}

func (router *ScoreBasedRouter) addConn(backend *backendWrapper, conn SimpleConn) {
	ce := backend.connList.PushBack(conn)
	router.setConnEl(conn, ce)
}

// RefreshBackend implements Router.GetBackendSelector interface.
func (router *ScoreBasedRouter) RefreshBackend() {
	router.observer.Refresh()
}

func (router *ScoreBasedRouter) ensureBackend(addr string) *backendWrapper {
	backend, ok := router.backends[addr]
	if ok {
		return backend
	}
	// The backend should always exist if it will be needed. Add a warning and add it back.
	router.logger.Warn("backend is not found in the router", zap.String("backend_addr", addr), zap.Stack("stack"))
	ip, _, _ := net.SplitHostPort(addr)
	backend = newBackendWrapper(addr, observer.BackendHealth{
		BackendInfo: observer.BackendInfo{
			IP:         ip,
			StatusPort: 10080, // impossible anyway
		},
		Healthy: false,
	})
	router.backends[addr] = backend
	return backend
}

// OnConnClosed implements ConnEventReceiver.OnConnClosed interface.
func (router *ScoreBasedRouter) OnConnClosed(addr string, conn SimpleConn) error {
	router.Lock()
	defer router.Unlock()
	backend := router.ensureBackend(addr)
	connEl := router.getConnEl(conn)
	backend.connScore--
	router.removeConn(backend, connEl)
	return nil
}

func (router *ScoreBasedRouter) updateBackendHealth(healthResults observer.HealthResult) {
	router.Lock()
	defer router.Unlock()
	router.observeError = healthResults.Error()
	if router.observeError != nil {
		return
	}

	// `backends` contain all the backends, not only the updated ones.
	backends := healthResults.Backends()
	// If some backends are removed from the list, add them to `backends`.
	for addr, backend := range router.backends {
		if _, ok := backends[addr]; !ok {
			health := backend.getHealth()
			router.logger.Debug("backend is removed from the list, add it back to router", zap.String("addr", addr), zap.Stringer("health", &health))
			backends[addr] = &observer.BackendHealth{
				BackendInfo: backend.GetBackendInfo(),
				Healthy:     false,
				PingErr:     errors.New("removed from backend list"),
			}
		}
	}
	var serverVersion string
	for addr, health := range backends {
		backend, ok := router.backends[addr]
		if !ok && health.Healthy {
			router.logger.Debug("add new backend to router", zap.String("addr", addr), zap.Stringer("health", health))
			router.backends[addr] = newBackendWrapper(addr, *health)
		} else if ok {
			if !health.Equals(backend.getHealth()) {
				router.logger.Debug("update backend in router", zap.String("addr", addr), zap.Stringer("health", health))
			}
			backend.setHealth(*health)
			router.removeBackendIfEmpty(backend)
		} else {
			router.logger.Debug("unhealthy backend is not in router", zap.String("addr", addr), zap.Stringer("health", health))
		}
	}
	if len(serverVersion) > 0 {
		router.serverVersion = serverVersion
	}
}

func (router *ScoreBasedRouter) refreshBackendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case healthResults := <-router.healthCh:
			router.updateBackendHealth(healthResults)
		}
	}
}

func (router *ScoreBasedRouter) removeBackendIfEmpty(backend *backendWrapper) bool {
	// If connList.Len() == 0, there won't be any outgoing connections.
	// And if also connScore == 0, there won't be any incoming connections.
	if !backend.Healthy() && backend.connList.Len() == 0 && backend.connScore <= 0 {
		delete(router.backends, backend.addr)
		return true
	}
	return false
}

func (router *ScoreBasedRouter) ConnCount() int {
	router.Lock()
	defer router.Unlock()
	j := 0
	for _, backend := range router.backends {
		j += backend.connList.Len()
	}
	return j
}

// Close implements Router.Close interface.
func (router *ScoreBasedRouter) Close() {
	if router.cancelFunc != nil {
		router.cancelFunc()
		router.cancelFunc = nil
	}
	router.wg.Wait()
	if router.observer != nil {
		router.observer.Unsubscribe("score_based_router")
	}
	// Router only refers to SimpleConn, it doesn't manage SimpleConn.
}
