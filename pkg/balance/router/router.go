// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"sync"

	glist "github.com/bahlo/generic-list-go"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"go.uber.org/zap"
)

var (
	ErrNoBackend = errors.New("no available backend")
)

// ConnEventReceiver receives connection events.
type ConnEventReceiver interface {
	OnConnClosed(addr string, conn SimpleConn) error
}

// Router routes client connections to backends.
type Router interface {
	// ConnEventReceiver handles connection events to balance connections if possible.
	ConnEventReceiver

	GetBackendSelector() BackendSelector
	HealthyBackendCount() int
	RefreshBackend()
	ConnCount() int
	Close()
}

// SimpleConn indicates a simple connection.
type SimpleConn interface {
	SetEventReceiver(receiver ConnEventReceiver)
	SetValue(key, val any)
	Value(key any) any
	ConnectionID() uint64
	ConnInfo() []zap.Field
}

// BackendInst defines a backend refer a connection
type BackendInst interface {
	Addr() string
	Healthy() bool
}

// backendWrapper contains the connections on the backend.
type backendWrapper struct {
	mu struct {
		sync.RWMutex
		observer.BackendHealth
	}
	addr string
	// connScore is used for calculating backend scores and check if the backend can be removed from the list.
	// connScore = connList.Len() + incoming connections - outgoing connections.
	connScore int
	// A list of SimpleConn and is ordered by the connecting time.
	// connList only includes the connections that are currently on this backend.
	connList *glist.List[SimpleConn]
}

func newBackendWrapper(addr string, health observer.BackendHealth) *backendWrapper {
	wrapper := &backendWrapper{
		addr:     addr,
		connList: glist.New[SimpleConn](),
	}
	wrapper.setHealth(health)
	return wrapper
}

func (b *backendWrapper) setHealth(health observer.BackendHealth) {
	b.mu.Lock()
	b.mu.BackendHealth = health
	b.mu.Unlock()
}

func (b *backendWrapper) getHealth() observer.BackendHealth {
	b.mu.RLock()
	health := b.mu.BackendHealth
	b.mu.RUnlock()
	return health
}

func (b *backendWrapper) ConnScore() int {
	return b.connScore
}

func (b *backendWrapper) Addr() string {
	return b.addr
}

func (b *backendWrapper) Healthy() bool {
	b.mu.RLock()
	healthy := b.mu.Healthy
	b.mu.RUnlock()
	return healthy
}

func (b *backendWrapper) ConnCount() int {
	return b.connList.Len()
}

func (b *backendWrapper) GetBackendInfo() observer.BackendInfo {
	b.mu.RLock()
	info := b.mu.BackendInfo
	b.mu.RUnlock()
	return info
}

func (b *backendWrapper) Equals(health observer.BackendHealth) bool {
	b.mu.RLock()
	equal := b.mu.BackendHealth.Equals(health)
	b.mu.RUnlock()
	return equal
}

func (b *backendWrapper) String() string {
	b.mu.RLock()
	str := b.mu.String()
	b.mu.RUnlock()
	return str
}
