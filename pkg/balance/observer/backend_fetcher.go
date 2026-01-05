// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
)

var _ BackendFetcher = (*StaticFetcher)(nil)

// BackendFetcher is an interface to fetch the backend list.
type BackendFetcher interface {
	GetBackendList(context.Context) (map[string]*BackendInfo, error)
}

// StaticFetcher uses configured static addrs. This is only used for testing.
type StaticFetcher struct {
	backends map[string]*BackendInfo
}

func NewStaticFetcher(staticAddrs []string) *StaticFetcher {
	return &StaticFetcher{
		backends: backendListToMap(staticAddrs),
	}
}

func (sf *StaticFetcher) GetBackendList(context.Context) (map[string]*BackendInfo, error) {
	return sf.backends, nil
}

func backendListToMap(addrs []string) map[string]*BackendInfo {
	backends := make(map[string]*BackendInfo, len(addrs))
	for _, addr := range addrs {
		backends[addr] = &BackendInfo{}
	}
	return backends
}
