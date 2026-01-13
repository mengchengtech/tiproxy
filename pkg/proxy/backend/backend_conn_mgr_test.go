// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/logger"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/mengchengtech/cerberus/pkg/balance/router"
	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	eventSucceed = iota
	eventFail
	eventClose
)

type event struct {
	addr      string
	eventName int
}

type mockEventReceiver struct {
	eventCh chan event
}

func newMockEventReceiver() *mockEventReceiver {
	return &mockEventReceiver{
		eventCh: make(chan event, 1),
	}
}

func (mer *mockEventReceiver) OnConnClosed(addr string, conn router.SimpleConn) error {
	mer.eventCh <- event{
		addr:      addr,
		eventName: eventClose,
	}
	return nil
}

func (mer *mockEventReceiver) checkEvent(t *testing.T, eventName int) event {
	e := <-mer.eventCh
	require.Equal(t, eventName, e.eventName)
	return e
}

type runner struct {
	client  func(packetIO pnet.PacketIO) error
	proxy   func(clientIO, backendIO pnet.PacketIO) error
	backend func(packetIO pnet.PacketIO) error
}

// backendMgrTester encapsulates testSuite but is dedicated for BackendConnMgr.
type backendMgrTester struct {
	*testSuite
	t      *testing.T
	lg     *zap.Logger
	closed bool
}

func newBackendMgrTester(t *testing.T, cfg ...cfgOverrider) *backendMgrTester {
	tc := newTCPConnSuite(t)
	cfg = append(cfg, func(cfg *testConfig) {
		cfg.testSuiteConfig.enableRouteLogic = true
	})
	ts, clean := newTestSuite(t, tc, cfg...)
	lg, _ := logger.CreateLoggerForTest(t)
	tester := &backendMgrTester{
		testSuite: ts,
		t:         t,
		lg:        lg,
	}
	t.Cleanup(func() {
		clean()
		if tester.closed {
			return
		}
		err := ts.mp.Close()
		require.NoError(t, err)
		eventReceiver := ts.mp.getEventReceiver()
		if eventReceiver != nil {
			eventReceiver.(*mockEventReceiver).checkEvent(t, eventClose)
		}
	})
	return tester
}

// Define some common runners here to reduce code redundancy.
func (ts *backendMgrTester) firstHandshake4Proxy(clientIO, backendIO pnet.PacketIO) error {
	err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig, ts.mp.username, ts.mp.password)
	require.NoError(ts.t, err)
	mer := newMockEventReceiver()
	ts.mp.SetEventReceiver(mer)
	return nil
}

func (ts *backendMgrTester) handshake4Backend(packetIO pnet.PacketIO) error {
	conn, err := ts.tc.backendListener.Accept()
	require.NoError(ts.t, err)
	ts.tc.backendIO = pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
	return ts.mb.authenticate(ts.tc.backendIO)
}

func (ts *backendMgrTester) forwardCmd4Proxy(clientIO, backendIO pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	require.NoError(ts.t, err)
	rsErr := ts.mp.ExecuteCmd(context.Background(), request)
	if pnet.IsMySQLError(rsErr) {
		rsErr = nil
	}
	return rsErr
}

func (ts *backendMgrTester) respondWithNoTxn4Backend(packetIO pnet.PacketIO) error {
	ts.mb.respondType = responseTypeOK
	ts.mb.status = 0
	return ts.mb.respond(packetIO)
}

func (ts *backendMgrTester) startTxn4Backend(packetIO pnet.PacketIO) error {
	ts.mb.respondType = responseTypeOK
	ts.mb.status = pnet.ServerStatusInTrans
	return ts.mb.respond(packetIO)
}

func (ts *backendMgrTester) checkConnClosed4Proxy(_, _ pnet.PacketIO) error {
	require.Eventually(ts.t, func() bool {
		switch ts.mp.closeStatus.Load() {
		case statusClosing, statusClosed:
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)
	return nil
}

func (ts *backendMgrTester) runTests(runners []runner) {
	for _, runner := range runners {
		ts.runAndCheck(ts.t, nil, runner.client, runner.backend, runner.proxy)
		require.Equal(ts.t, ts.tc.clientIO.InBytes(), ts.mp.ClientOutBytes())
		require.Equal(ts.t, ts.tc.clientIO.OutBytes(), ts.mp.ClientInBytes())
	}
}

// Test that the client handshake fails.
func TestConnectFail(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				return ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig, "", "")
			},
			backend: func(_ pnet.PacketIO) error {
				conn, err := ts.tc.backendListener.Accept()
				require.NoError(ts.t, err)
				ts.tc.backendIO = pnet.NewPacketIO(conn, ts.lg, pnet.DefaultConnBufferSize)
				ts.mb.authSucceed = false
				return ts.mb.authenticate(ts.tc.backendIO)
			},
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, SrcClientAuthFail, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

// Test that the proxy sends the right handshake info after COM_CHANGE_USER and COM_SET_OPTION.
func TestSpecialCmds(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// change user
		{
			client: func(packetIO pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComChangeUser
				ts.mc.username = "another_user"
				ts.mc.dbName = "another_db"
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// disable multi-stmts
		{
			client: func(packetIO pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComSetOption
				ts.mc.dataBytes = []byte{1, 0}
				return ts.mc.request(packetIO)
			},
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
	}
	ts.runTests(runners)
}

func TestCustomHandshake(t *testing.T) {
	ts := newBackendMgrTester(t, func(cfg *testConfig) {
		handler := cfg.proxyConfig.handler
		handler.handleHandshakeResp = func(ctx ConnContext, resp *pnet.HandshakeResp) error {
			resp.User = "rewritten_user"
			resp.Attrs = map[string]string{"key": "value"}
			return nil
		}
		handler.getCapability = func() pnet.Capability {
			return SupportedServerCapabilities & ^pnet.ClientDeprecateEOF
		}
		handler.getServerVersion = func() string {
			return "test_server_version"
		}
	})
	runners := []runner{
		// 1st handshake
		{
			client: func(packetIO pnet.PacketIO) error {
				if err := ts.mc.authenticate(packetIO); err != nil {
					return err
				}
				require.Equal(t, "test_server_version", ts.mc.serverVersion)
				return nil
			},
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// query
		{
			client: func(packetIO pnet.PacketIO) error {
				ts.mc.sql = "select 1"
				return ts.mc.request(packetIO)
			},
			proxy: ts.forwardCmd4Proxy,
			backend: func(packetIO pnet.PacketIO) error {
				ts.mb.respondType = responseTypeResultSet
				ts.mb.columns = 1
				ts.mb.rows = 1
				return ts.mb.respond(packetIO)
			},
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, SrcNone, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseWhenIdle(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// graceful close
		{
			proxy: func(_, _ pnet.PacketIO) error {
				ts.mp.GracefulClose()
				return nil
			},
		},
		// really closed
		{
			proxy: ts.checkConnClosed4Proxy,
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseWhenActive(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// start a transaction to make it active
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.startTxn4Backend,
		},
		// try to gracefully close but it doesn't close
		{
			proxy: func(_, _ pnet.PacketIO) error {
				ts.mp.GracefulClose()
				time.Sleep(300 * time.Millisecond)
				require.Equal(t, statusNotifyClose, ts.mp.closeStatus.Load())
				return nil
			},
		},
		// finish the transaction
		{
			client:  ts.mc.request,
			proxy:   ts.forwardCmd4Proxy,
			backend: ts.respondWithNoTxn4Backend,
		},
		// it will then automatically close
		{
			proxy: ts.checkConnClosed4Proxy,
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestGracefulCloseBeforeHandshake(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// try to gracefully close before handshake
		{
			proxy: func(_, _ pnet.PacketIO) error {
				ts.mp.GracefulClose()
				return nil
			},
		},
		// connect fails
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig, "", "")
				require.Error(ts.t, err)
				require.Equal(t, SrcProxyQuit, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestHandlerReturnError(t *testing.T) {
	tests := []struct {
		cfg        cfgOverrider
		errMsg     string
		quitSource ErrorSource
	}{
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.handler.getRouter = func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
					return nil, errors.New("mocked error")
				}
			},
			errMsg:     "mocked error",
			quitSource: SrcProxyErr,
		},
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.handler.handleHandshakeResp = func(ctx ConnContext, resp *pnet.HandshakeResp) error {
					return errors.New("mocked error")
				}
			},
			errMsg:     "mocked error",
			quitSource: SrcProxyErr,
		},
		{
			cfg: func(config *testConfig) {
				config.proxyConfig.bcConfig.ConnectTimeout = time.Second
				config.proxyConfig.handler.getRouter = func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
					return router.NewStaticRouter(nil), nil
				}
			},
			errMsg:     ErrProxyNoBackend.Error(),
			quitSource: SrcProxyNoBackend,
		},
	}
	for _, test := range tests {
		ts := newBackendMgrTester(t, test.cfg)
		rn := runner{
			client: func(packetIO pnet.PacketIO) error {
				err := ts.mc.authenticate(packetIO)
				require.NoError(t, err)
				require.ErrorContains(t, ts.mc.mysqlErr, test.errMsg)
				return nil
			},
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig, "", "")
				require.Error(t, err)
				require.Equal(t, test.quitSource, ts.mp.QuitSource())
				return nil
			},
			backend: nil,
		}
		ts.runAndCheck(ts.t, func(t *testing.T, ts *testSuite) {}, rn.client, rn.backend, rn.proxy)
	}
}

func TestGetBackendIO(t *testing.T) {
	addrs := make([]string, 0, 3)
	listeners := make([]net.Listener, 0, cap(addrs))

	for i := 0; i < cap(addrs); i++ {
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(t, err)
		listeners = append(listeners, listener)
		addrs = append(addrs, listener.Addr().String())
	}

	rt := router.NewStaticRouter(addrs)
	badAddrs := make(map[string]struct{}, 3)
	handler := &CustomHandshakeHandler{
		getRouter: func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
			return rt, nil
		},
		onHandshake: func(connContext ConnContext, s string, err error, src ErrorSource) {
			if err != nil && len(s) > 0 {
				badAddrs[s] = struct{}{}
			}
			if err != nil {
				require.Equal(t, SrcBackendHandshake, src)
			}
		},
	}
	lg, _ := logger.CreateLoggerForTest(t)
	mgr := NewBackendConnManager(lg, handler, 0, &BCConfig{ConnectTimeout: time.Second})
	var wg waitgroup.WaitGroup
	for i := 0; i <= len(listeners); i++ {
		wg.Run(func() {
			if i < len(listeners) {
				cn, err := listeners[i].Accept()
				require.NoError(t, err)
				require.NoError(t, cn.Close())
			}
		})
		io, err := mgr.getBackendIO(context.Background(), mgr, nil)
		if err == nil {
			require.NoError(t, io.Close())
		}
		message := fmt.Sprintf("%d: %s, %+v\n", i, badAddrs, err)
		if i < len(listeners) {
			require.NoError(t, err, message)
			err = listeners[i].Close()
			require.NoError(t, err, message)
		} else {
			require.Error(t, err, message)
		}
		require.True(t, len(badAddrs) <= i, message)
		badAddrs = make(map[string]struct{}, 3)
		wg.Wait()
	}
}

func TestBackendInactive(t *testing.T) {
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.bcConfig.TickerInterval = time.Millisecond
		config.proxyConfig.bcConfig.CheckBackendInterval = 10 * time.Millisecond
	})
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// do some queries and the interval is less than checkBackendInterval
		{
			client: func(packetIO pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					time.Sleep(5 * time.Millisecond)
					if err := ts.mc.request(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					if err := ts.forwardCmd4Proxy(clientIO, backendIO); err != nil {
						return err
					}
				}
				return nil
			},
			backend: func(packetIO pnet.PacketIO) error {
				for i := 0; i < 10; i++ {
					if err := ts.respondWithNoTxn4Backend(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
		},
		// do some queries and the interval is longer than checkBackendInterval
		{
			client: func(packetIO pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					time.Sleep(30 * time.Millisecond)
					if err := ts.mc.request(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					if err := ts.forwardCmd4Proxy(clientIO, backendIO); err != nil {
						return err
					}
				}
				return nil
			},
			backend: func(packetIO pnet.PacketIO) error {
				for i := 0; i < 5; i++ {
					if err := ts.respondWithNoTxn4Backend(packetIO); err != nil {
						return err
					}
				}
				return nil
			},
		},
		// close the backend and the client connection will close
		{
			proxy: ts.checkConnClosed4Proxy,
			backend: func(packetIO pnet.PacketIO) error {
				return packetIO.Close()
			},
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, SrcBackendNetwork, ts.mp.QuitSource())
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestKeepAlive(t *testing.T) {
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.bcConfig.TickerInterval = time.Millisecond
		config.proxyConfig.bcConfig.HealthyKeepAlive.Idle = time.Minute
		config.proxyConfig.bcConfig.UnhealthyKeepAlive.Idle = time.Second
	})
	runners := []runner{
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.NoError(t, ts.firstHandshake4Proxy(clientIO, backendIO))
				require.Equal(t, time.Minute, (*ts.mp.backendIO.Load()).LastKeepAlive().Idle)
				return nil
			},
			backend: ts.handshake4Backend,
		},
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				require.Equal(t, time.Minute, (*ts.mp.backendIO.Load()).LastKeepAlive().Idle)
				ts.mp.curBackend.(*router.StaticBackend).SetHealthy(false)
				require.Eventually(t, func() bool {
					return (*ts.mp.backendIO.Load()).LastKeepAlive().Idle == time.Second
				}, 3*time.Second, 10*time.Millisecond)
				ts.mp.curBackend.(*router.StaticBackend).SetHealthy(true)
				require.Eventually(t, func() bool {
					return (*ts.mp.backendIO.Load()).LastKeepAlive().Idle == time.Minute
				}, 3*time.Second, 10*time.Millisecond)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestConnID(t *testing.T) {
	ids := []uint64{0, 4, 9}
	for _, id := range ids {
		ts := newBackendMgrTester(t, func(config *testConfig) {
			config.proxyConfig.connectionID = id
		})
		runners := []runner{{
			client: func(packetIO pnet.PacketIO) error {
				err := ts.mc.authenticate(packetIO)
				require.Equal(t, ts.mc.connid, id)
				return err
			},
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		}}
		ts.runTests(runners)
	}
}

func TestConnAttrs(t *testing.T) {
	ts := newBackendMgrTester(t)
	attr1 := map[string]string{"k1": "v1"}
	attr2 := map[string]string{"k2": "v2"}
	runners := []runner{
		// 1st handshake
		{
			client: func(packetIO pnet.PacketIO) error {
				ts.mc.attrs = attr1
				return ts.mc.authenticate(packetIO)
			},
			proxy: ts.firstHandshake4Proxy,
			backend: func(packetIO pnet.PacketIO) error {
				err := ts.handshake4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr1, ts.mb.attrs)
				return nil
			},
		},
		// CHANGE_USER updates attrs
		{
			client: func(packetIO pnet.PacketIO) error {
				ts.mc.cmd = pnet.ComChangeUser
				ts.mc.attrs = attr2
				return ts.mc.request(packetIO)
			},
			proxy: ts.forwardCmd4Proxy,
			backend: func(packetIO pnet.PacketIO) error {
				err := ts.respondWithNoTxn4Backend(packetIO)
				require.NoError(t, err)
				require.Equal(t, attr2, ts.mb.attrs)
				return nil
			},
		},
	}
	ts.runTests(runners)
}

func TestCloseWhileConnect(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake while force close
		{
			client: ts.mc.authenticate,
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				err := ts.mp.Connect(context.Background(), clientIO, ts.mp.frontendTLSConfig, ts.mp.backendTLSConfig, "", "")
				if err == nil {
					mer := newMockEventReceiver()
					ts.mp.SetEventReceiver(mer)
				}
				return err
			},
			backend: ts.handshake4Backend,
		},
	}

	ts.runTests(runners)
}

func TestCloseWhileExecute(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// execute cmd while force close
		{
			client: ts.mc.request,
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				clientIO.ResetSequence()
				request, err := clientIO.ReadPacket()
				if err != nil {
					return err
				}
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				return ts.mp.ExecuteCmd(context.Background(), request)
			},
			backend: ts.startTxn4Backend,
		},
	}

	ts.runTests(runners)
}

func TestCloseWhileGracefulClose(t *testing.T) {
	ts := newBackendMgrTester(t)
	runners := []runner{
		// 1st handshake
		{
			client:  ts.mc.authenticate,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
		// graceful close while force close
		{
			proxy: func(clientIO, backendIO pnet.PacketIO) error {
				go func() {
					require.NoError(ts.t, ts.mp.BackendConnManager.Close())
				}()
				ts.mp.GracefulClose()
				return nil
			},
		},
	}

	ts.runTests(runners)
}

func TestDisconnectLog(t *testing.T) {
	ts := newBackendMgrTester(t)
	tests := []struct {
		runner  runner
		checker checker
	}{
		{
			// 1st handshake
			runner: runner{
				client:  ts.mc.authenticate,
				proxy:   ts.firstHandshake4Proxy,
				backend: ts.handshake4Backend,
			},
		},
		{
			// proxy logs SQL when the backend disconnects
			runner: runner{
				client: func(packetIO pnet.PacketIO) error {
					ts.mc.sql = "select 1"
					return ts.mc.request(packetIO)
				},
				proxy: func(clientIO, backendIO pnet.PacketIO) error {
					err := ts.forwardCmd4Proxy(clientIO, backendIO)
					_ = clientIO.Close()
					return err
				},
				backend: func(packetIO pnet.PacketIO) error {
					return packetIO.Close()
				},
			},
			checker: func(t *testing.T, ts *testSuite) {
				require.True(t, pnet.IsDisconnectError(ts.mc.err))
				require.ErrorIs(t, ts.mp.err, ErrBackendConn)
				require.True(t, strings.Contains(ts.mp.text.String(), "select ?"))
			},
		},
	}
	// Do not run ts.runTests(runners) to skip the general checker.
	for _, test := range tests {
		ts.runAndCheck(ts.t, test.checker, test.runner.client, test.runner.backend, test.runner.proxy)
	}
}

func TestConnectWithBackend(t *testing.T) {
	ts := newBackendMgrTester(t, func(config *testConfig) {
		config.proxyConfig.username = "u1"
		config.proxyConfig.password = "fake_password"
	})
	runners := []runner{
		{
			client:  nil,
			proxy:   ts.firstHandshake4Proxy,
			backend: ts.handshake4Backend,
		},
	}
	ts.runTests(runners)
}

func BenchmarkSyncMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var m sync.Map
		m.Store("1", "1")
		m.Load("1")
	}
}

func BenchmarkLockedMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := make(map[string]string)
		var lock sync.Mutex
		lock.Lock()
		m["1"] = "1"
		_ = m["1"]
		lock.Unlock()
	}
}
