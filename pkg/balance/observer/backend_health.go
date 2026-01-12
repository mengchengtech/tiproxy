// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"fmt"
	"strings"
)

type BackendHealth struct {
	BackendInfo
	Healthy bool
	// The error occurred when health check fails. It's used to log why the backend becomes unhealthy.
	PingErr error
	// The backend version that returned to the client during handshake.
	ServerVersion string
}

func (bh *BackendHealth) Equals(other BackendHealth) bool {
	return bh.BackendInfo.Equals(other.BackendInfo) &&
		bh.Healthy == other.Healthy &&
		bh.ServerVersion == other.ServerVersion
}

func (bh *BackendHealth) String() string {
	var sb strings.Builder
	if bh.Healthy {
		_, _ = sb.WriteString("healthy")
	} else {
		_, _ = sb.WriteString("down")
	}
	if bh.PingErr != nil {
		_, _ = sb.WriteString(fmt.Sprintf(", err: %s", bh.PingErr.Error()))
	}
	if len(bh.ServerVersion) > 0 {
		_, _ = sb.WriteString(", version: ")
		_, _ = sb.WriteString(bh.ServerVersion)
	}
	return sb.String()
}

// BackendInfo stores the status info of each backend.
type BackendInfo struct {
	IP         string
	StatusPort uint
}

func (bi BackendInfo) Equals(other BackendInfo) bool {
	return bi.IP == other.IP &&
		bi.StatusPort == other.StatusPort
}

// HealthResult contains the health check results and is used to notify the routers.
// It's read-only for subscribers.
type HealthResult struct {
	// `backends` is empty when `err` is not nil. It doesn't mean there are no backends.
	backends map[string]*BackendHealth
	err      error
}

// NewHealthResult is used for testing in other packages.
func NewHealthResult(backends map[string]*BackendHealth, err error) HealthResult {
	return HealthResult{
		backends: backends,
		err:      err,
	}
}

func (hr HealthResult) Backends() map[string]*BackendHealth {
	newMap := make(map[string]*BackendHealth, len(hr.backends))
	for addr, health := range hr.backends {
		newMap[addr] = health
	}
	return newMap
}

func (hr HealthResult) Error() error {
	return hr.err
}
