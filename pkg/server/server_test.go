// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"os"
	"testing"

	"github.com/mengchengtech/cerberus/lib/util/logger"
	"github.com/mengchengtech/cerberus/pkg/sctx"
	"github.com/mengchengtech/cerberus/pkg/util/etcd"
	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	etcdServer, err := etcd.CreateEtcdServer("0.0.0.0:0", dir, lg)
	require.NoError(t, err)
	configFile := dir + "/config.toml"
	endpoint := etcdServer.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)
	b, err := toml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configFile, b, 0o644))

	server, err := NewServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)
	require.NoError(t, server.Close())
	etcdServer.Close()
}
