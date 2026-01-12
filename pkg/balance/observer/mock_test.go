// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"sync"
)

type mockBackendFetcher struct {
	sync.Mutex
	backends map[string]*BackendInfo
}

func newMockBackendFetcher() *mockBackendFetcher {
	return &mockBackendFetcher{
		backends: make(map[string]*BackendInfo),
	}
}

func (mbf *mockBackendFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	mbf.Lock()
	defer mbf.Unlock()
	backends := make(map[string]*BackendInfo, len(mbf.backends))
	for addr, backend := range mbf.backends {
		backends[addr] = backend
	}
	return backends, nil
}

func (mbf *mockBackendFetcher) setBackend(addr string, info *BackendInfo) {
	mbf.Lock()
	defer mbf.Unlock()
	mbf.backends[addr] = info
}

func (mbf *mockBackendFetcher) removeBackend(addr string) {
	mbf.Lock()
	defer mbf.Unlock()
	delete(mbf.backends, addr)
}

type mockHealthCheck struct {
	sync.Mutex
	backends map[string]*BackendHealth
}

func newMockHealthCheck() *mockHealthCheck {
	return &mockHealthCheck{
		backends: make(map[string]*BackendHealth),
	}
}

func (mhc *mockHealthCheck) Check(_ context.Context, addr string, info *BackendInfo) *BackendHealth {
	mhc.Lock()
	defer mhc.Unlock()
	mhc.backends[addr].BackendInfo = *info
	return mhc.backends[addr]
}

func (mhc *mockHealthCheck) setBackend(addr string, health *BackendHealth) {
	mhc.Lock()
	defer mhc.Unlock()
	mhc.backends[addr] = health
}

func (mhc *mockHealthCheck) setHealth(addr string, healthy bool) {
	mhc.Lock()
	defer mhc.Unlock()
	health := *mhc.backends[addr]
	health.Healthy = healthy
	mhc.backends[addr] = &health
}

func (mhc *mockHealthCheck) removeBackend(addr string) {
	mhc.Lock()
	defer mhc.Unlock()
	delete(mhc.backends, addr)
}
