// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

var testProxyConfig = Config{
	Workdir: "./wd",
	Proxy: ProxyServer{
		Addr:                       "0.0.0.0:4000",
		MaxConnections:             1,
		FrontendKeepalive:          KeepAlive{Enabled: true},
		ProxyProtocol:              "v2",
		GracefulWaitBeforeShutdown: 10,
		ConnBufferSize:             32 * 1024,
	},
	Log: Log{
		Encoder: "tidb",
		LogOnline: LogOnline{
			Level: "info",
			LogFile: LogFile{
				Filename:   ".",
				MaxSize:    10,
				MaxDays:    1,
				MaxBackups: 1,
			},
		},
	},
	Security: Security{
		ServerSQLTLS: TLSConfig{
			CA:        "a",
			Cert:      "b",
			Key:       "c",
			AutoCerts: true,
		},
		SQLTLS: TLSConfig{
			CA:                 "a",
			RSAKeySize:         0,
			AutoExpireDuration: "1y",
			SkipCA:             true,
			Cert:               "b",
			Key:                "c",
		},
		RequireBackendTLS: true,
	},
}

func TestProxyConfig(t *testing.T) {
	data1, err := testProxyConfig.ToBytes()
	require.NoError(t, err)
	var cfg Config
	err = toml.Unmarshal(data1, &cfg)
	require.NoError(t, err)
	data2, err := cfg.ToBytes()
	require.NoError(t, err)
	require.Equal(t, data1, data2)
}

func TestProxyCheck(t *testing.T) {
	testcases := []struct {
		pre  func(*testing.T, *Config)
		post func(*testing.T, *Config)
		err  error
	}{
		{
			pre: func(t *testing.T, c *Config) {
				c.Workdir = ""
			},
			post: func(t *testing.T, c *Config) {
				cwd, err := os.Getwd()
				require.NoError(t, err)
				require.Equal(t, filepath.Clean(filepath.Join(cwd, "work")), c.Workdir)
			},
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.ProxyProtocol = "v1"
			},
			err: ErrUnsupportedProxyProtocolVersion,
		},
		{
			pre: func(t *testing.T, c *Config) {
				c.Proxy.ConnBufferSize = 100 * 1024 * 1024
			},
			err: ErrInvalidConfigValue,
		},
	}
	for _, tc := range testcases {
		cfg := testProxyConfig
		tc.pre(t, &cfg)
		if tc.err != nil {
			require.ErrorIs(t, cfg.Check(), tc.err)
			continue
		}
		require.NoError(t, cfg.Check())
		tc.post(t, &cfg)
	}
}

func TestGetIPPort(t *testing.T) {
	for _, cas := range []struct {
		addr       string
		port       string
		nonUnicast bool
	}{
		{":34", "34", true},
		{"0.0.0.0:34", "34", true},
		{"255.255.255.255:34", "34", true},
		{"239.255.255.255:34", "34", true},
		{"[FF02::1:FF47]:34", "34", true},
		{"127.0.0.1:34", "34", false},
		{"[F02::1:FF47]:34", "34", false},
		{"192.0.0.1:6049", "6049", false},
		{"0.0.0.0:1000", "1000", false},
	} {
		cfg := &Config{
			Proxy: ProxyServer{
				Addr: cas.addr,
			},
		}
		_, port, err := cfg.GetIPPort()
		require.NoError(t, err)

		require.Equal(t, cas.port, port)
	}
}
