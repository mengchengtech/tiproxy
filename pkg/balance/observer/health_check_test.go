// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/mengchengtech/cerberus/pkg/testkit"
	"github.com/mengchengtech/cerberus/pkg/util/logger"
	"github.com/mengchengtech/cerberus/pkg/util/waitgroup"
	"github.com/stretchr/testify/require"
)

// Test that the backend status is correct when the backend starts or shuts down.
func TestHealthCheck(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	hc := NewDefaultHealthCheck(cfg, lg)
	backend, info := newBackendServer(t)
	health := hc.Check(context.Background(), backend.sqlAddr, info)
	require.True(t, health.Healthy)

	backend.stopSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.False(t, health.Healthy)
	backend.startSQLServer()
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.True(t, health.Healthy)

	backend.setSqlResp(false)
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.False(t, health.Healthy)
	backend.setSqlResp(true)
	health = hc.Check(context.Background(), backend.sqlAddr, info)
	require.True(t, health.Healthy)
	backend.close()
}

type backendServer struct {
	t           *testing.T
	sqlListener net.Listener
	sqlAddr     string
	wg          waitgroup.WaitGroup
	ip          string
	statusPort  uint
	sqlResp     atomic.Bool
}

func newBackendServer(t *testing.T) (*backendServer, *BackendInfo) {
	backend := &backendServer{
		t: t,
	}
	backend.setSqlResp(true)
	backend.startSQLServer()
	return backend, &BackendInfo{
		IP:         backend.ip,
		StatusPort: backend.statusPort,
	}
}

func (srv *backendServer) setSqlResp(sqlResp bool) {
	srv.sqlResp.Store(sqlResp)
}

func (srv *backendServer) startSQLServer() {
	srv.sqlListener, srv.sqlAddr = testkit.StartListener(srv.t, srv.sqlAddr)
	srv.wg.Run(func() {
		for {
			conn, err := srv.sqlListener.Accept()
			if err != nil {
				// listener is closed
				break
			}
			if srv.sqlResp.Load() {
				data := []byte{0, 0, 0, 0, 0}
				c := packet.NewConn(conn)
				require.NoError(srv.t, c.WritePacket(data))
			}
			_ = conn.Close()
		}
	})
}

func (srv *backendServer) stopSQLServer() {
	err := srv.sqlListener.Close()
	require.NoError(srv.t, err)
}

func (srv *backendServer) close() {
	srv.stopSQLServer()
	srv.wg.Wait()
}
