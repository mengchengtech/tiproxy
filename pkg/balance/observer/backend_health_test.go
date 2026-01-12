// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackendHealthToString(t *testing.T) {
	tests := []BackendHealth{
		{},
		{
			BackendInfo: BackendInfo{
				IP:         "127.0.0.1",
				StatusPort: 1,
			},
			Healthy: true,
			PingErr: errors.New("mock error"),
		},
	}
	// Just test no error happens
	for _, test := range tests {
		_ = test.String()
	}
}

func TestBackendHealthEquals(t *testing.T) {
	tests := []struct {
		a, b  BackendHealth
		equal bool
	}{
		{
			a:     BackendHealth{},
			b:     BackendHealth{},
			equal: true,
		},
		{
			a: BackendHealth{
				BackendInfo: BackendInfo{
					IP:         "127.0.0.1",
					StatusPort: 1,
				},
			},
			b: BackendHealth{
				BackendInfo: BackendInfo{
					IP:         "127.0.0.1",
					StatusPort: 1,
				},
			},
			equal: true,
		},
		{
			a: BackendHealth{
				BackendInfo: BackendInfo{
					IP:         "127.0.0.1",
					StatusPort: 1,
				},
				Healthy: true,
				PingErr: errors.New("mock error"),
			},
			b:     BackendHealth{},
			equal: false,
		},
	}
	// Just test no error happens
	for i, test := range tests {
		require.True(t, test.a.Equals(test.a), "test %d", i)
		require.True(t, test.b.Equals(test.b), "test %d", i)
		require.Equal(t, test.equal, test.a.Equals(test.b), "test %d", i)
		require.Equal(t, test.equal, test.b.Equals(test.a), "test %d", i)
	}
}
