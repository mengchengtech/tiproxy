// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/mengchengtech/cerberus/pkg/balance/router"
	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"github.com/pingcap/tidb/parser"
	"go.uber.org/zap"
)

var (
	ErrCloseConnMgr = errors.New("failed to close connection manager")
)

const (
	// DialTimeout is the timeout for each dial.
	DialTimeout = 1 * time.Second
	// ConnectTimeout is the timeout for choosing and connecting to an available backend.
	ConnectTimeout = 15 * time.Second
	// CheckBackendInterval is the interval for checking if the backend is still connected.
	CheckBackendInterval = time.Minute
	// TickerInterval is the interval for checking backend status.
	TickerInterval = 5 * time.Second
)

const (
	sqlQueryState    = "SHOW SESSION_STATES"
	sqlSetState      = "SET SESSION_STATES '%s'"
	sessionStatesCol = "Session_states"
	sessionTokenCol  = "Session_token"
)

type signalType int

const (
	signalTypeGracefulClose signalType = iota
	signalTypeNums
)

const (
	statusActive      int32 = iota
	statusNotifyClose       // notified to graceful close
	statusClosing           // really closing
	statusClosed
)

// Default values for ExponentialBackOff
// See https://pkg.go.dev/github.com/cenkalti/backoff/v4
const (
	defaultExponentialBackOffInitialInterval     = 100 * time.Millisecond
	defaultExponentialBackOffRandomizationFactor = 0.5
	defaultExponentialBackOffMultiplier          = 2
	defaultExponentionBackOffMaxInterval         = 4 * time.Second
)

type BCConfig struct {
	HealthyKeepAlive     config.KeepAlive
	UnhealthyKeepAlive   config.KeepAlive
	TickerInterval       time.Duration
	CheckBackendInterval time.Duration
	ConnectTimeout       time.Duration
	ConnBufferSize       int
	ProxyProtocol        bool
	RequireBackendTLS    bool
}

func (cfg *BCConfig) check() {
	if cfg.TickerInterval == time.Duration(0) {
		cfg.TickerInterval = TickerInterval
	}
	if cfg.CheckBackendInterval == time.Duration(0) {
		cfg.CheckBackendInterval = CheckBackendInterval
	}
	if cfg.ConnectTimeout == time.Duration(0) {
		cfg.ConnectTimeout = ConnectTimeout
	}
}

// BackendConnManager migrates a session from one BackendConnection to another.
//
// The signal processing goroutine tries to migrate the session once it receives a signal.
// If the session is not ready at that time, the cmd executing goroutine will try after executing commands.
//
// If it disconnects immediately: it's even worse than graceful shutdown.
// - If it retries after each command: the latency will be unacceptable afterwards if it always fails.
// - If it stops receiving signals: the previous new backend may be abnormal but the next new backend may be good.
type BackendConnManager struct {
	// processLock makes all processes exclusive.
	processLock sync.Mutex
	wg          waitgroup.WaitGroup
	// signalReceived is used to notify the signal processing goroutine.
	signalReceived chan signalType
	authenticator  *Authenticator
	cmdProcessor   *CmdProcessor
	eventReceiver  atomic.Pointer[router.ConnEventReceiver]
	config         *BCConfig
	logger         *zap.Logger
	curBackend     router.BackendInst
	// GracefulClose() sets it without lock.
	closeStatus atomic.Int32
	// The time when the connection was created.
	createTime time.Time
	// The last time when the backend is active.
	lastActiveTime time.Time
	// cancelFunc is used to cancel the signal processing goroutine.
	cancelFunc       context.CancelFunc
	clientIO         pnet.PacketIO
	backendIO        atomic.Pointer[pnet.PacketIO]
	backendTLS       *tls.Config
	handshakeHandler HandshakeHandler
	ctxmap           struct {
		sync.Mutex
		m map[any]any
	}
	connectionID uint64
	quitSource   ErrorSource
}

// NewBackendConnManager creates a BackendConnManager.
func NewBackendConnManager(logger *zap.Logger, handshakeHandler HandshakeHandler, connectionID uint64, config *BCConfig) *BackendConnManager {
	config.check()
	mgr := &BackendConnManager{
		logger:           logger,
		config:           config,
		connectionID:     connectionID,
		cmdProcessor:     NewCmdProcessor(logger.Named("cp")),
		handshakeHandler: handshakeHandler,
		authenticator:    NewAuthenticator(config),
		// There are 2 types of signals, which may be sent concurrently.
		signalReceived: make(chan signalType, signalTypeNums),
		quitSource:     SrcNone,
	}
	mgr.ctxmap.m = make(map[any]any)
	mgr.SetValue(ConnContextKeyConnID, connectionID)
	return mgr
}

// ConnectionID implements SimpleConn.ConnectionID interface.
// It returns the ID of the frontend connection. The ID stays still after session migration.
func (mgr *BackendConnManager) ConnectionID() uint64 {
	return mgr.connectionID
}

// Connect connects to the first backend.
func (mgr *BackendConnManager) Connect(ctx context.Context, clientIO pnet.PacketIO, frontendTLSConfig, backendTLSConfig *tls.Config, username, password string) error {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	mgr.backendTLS = backendTLSConfig
	mgr.clientIO = clientIO

	if mgr.closeStatus.Load() >= statusNotifyClose {
		mgr.quitSource = SrcProxyQuit
		return errors.New("graceful shutdown before connecting")
	}
	startTime := time.Now()
	mgr.createTime = startTime
	var err error
	if len(username) == 0 {
		// real client
		err = mgr.authenticator.handshakeFirstTime(ctx, mgr.logger.Named("authenticator"), mgr, clientIO, mgr.handshakeHandler, mgr.getBackendIO, frontendTLSConfig, backendTLSConfig)
	} else {
		// fake client, used for test
		err = mgr.authenticator.handshakeWithBackend(ctx, mgr.logger.Named("authenticator"), mgr, mgr.handshakeHandler, username, password, mgr.getBackendIO, backendTLSConfig)
	}
	if err != nil {
		src := Error2Source(err)
		mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), err, src)
		// For some errors, convert them to MySQL errors and send them to the client.
		if clientErr := ErrToClient(err); clientErr != nil {
			if writeErr := clientIO.WritePacket(pnet.MakeUserError(clientErr), true); writeErr != nil {
				mgr.logger.Warn("writing error to client failed", zap.NamedError("mysql_err", clientErr), zap.NamedError("write_err", writeErr))
			}
		}
		mgr.quitSource = src
		return err
	}
	mgr.handshakeHandler.OnHandshake(mgr, mgr.ServerAddr(), nil, SrcNone)
	endTime := time.Now()

	mgr.cmdProcessor.capability = mgr.authenticator.capability
	childCtx, cancelFunc := context.WithCancel(ctx)
	mgr.cancelFunc = cancelFunc
	mgr.lastActiveTime = endTime
	mgr.wg.RunWithRecover(func() {
		mgr.processSignals(childCtx)
	}, func(_ any) {
		// If we do not clean up, the router may retain the connection forever and the TiDB won't be released in the
		// Serverless Tier.
		_ = mgr.Close()
	}, mgr.logger)
	return nil
}

func (mgr *BackendConnManager) newExponentialBackOff() *backoff.ExponentialBackOff {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     defaultExponentialBackOffInitialInterval,
		RandomizationFactor: defaultExponentialBackOffRandomizationFactor,
		Multiplier:          defaultExponentialBackOffMultiplier,
		MaxInterval:         defaultExponentionBackOffMaxInterval,
		MaxElapsedTime:      mgr.config.ConnectTimeout,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return b
}

func (mgr *BackendConnManager) getBackendIO(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (pnet.PacketIO, error) {
	r, err := mgr.handshakeHandler.GetRouter(cctx, resp)
	if err != nil {
		return nil, errors.Wrap(err, ErrProxyErr)
	}
	// Reasons to wait:
	// - The TiDB instances may not be initialized yet
	// - One TiDB may be just shut down and another is just started but not ready yet
	bctx, cancel := context.WithTimeout(ctx, mgr.config.ConnectTimeout)
	selector := r.GetBackendSelector()
	startTime := time.Now()
	var addr string
	var backend router.BackendInst
	var origErr error
	io, err := backoff.RetryNotifyWithData(
		func() (pnet.PacketIO, error) {
			addr = ""
			// Try to connect to all backup backends one by one.
			if backend, err = selector.Next(); err == router.ErrNoBackend {
				return nil, ErrProxyNoBackend
			} else if err != nil {
				return nil, backoff.Permanent(errors.Wrap(err, ErrProxyErr))
			}

			var cn net.Conn
			addr = backend.Addr()
			cn, err = net.DialTimeout("tcp", addr, DialTimeout)
			selector.Finish(mgr, err == nil)
			if err != nil {
				return nil, errors.Wrap(errors.Wrapf(err, "dial backend %s error", addr), ErrBackendHandshake)
			}

			// NOTE: should use DNS name as much as possible
			// Usually certs are signed with domain instead of IP addrs
			// And `RemoteAddr()` will return IP addr
			backendIO := pnet.PacketIO(pnet.NewPacketIO(cn, mgr.logger, mgr.config.ConnBufferSize, pnet.WithRemoteAddr(addr, cn.RemoteAddr()), pnet.WithWrapError(ErrBackendConn)))
			mgr.backendIO.Store(&backendIO)
			mgr.curBackend = backend
			mgr.setKeepAlive()
			return backendIO, nil
		},
		backoff.WithContext(mgr.newExponentialBackOff(), bctx),
		func(err error, d time.Duration) {
			origErr = err
			mgr.handshakeHandler.OnHandshake(cctx, addr, err, Error2Source(err))
		},
	)
	cancel()

	duration := time.Since(startTime)
	if err != nil {
		mgr.logger.Error("get backend failed", zap.Duration("duration", duration), zap.NamedError("last_err", origErr))
	} else if duration >= time.Second {
		mgr.logger.Warn("get backend slow", zap.Duration("duration", duration), zap.NamedError("last_err", origErr),
			zap.String("backend_addr", mgr.ServerAddr()))
	}
	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		if origErr != nil {
			err = origErr
		}
	}
	return io, err
}

// ExecuteCmd forwards messages between the client and the backend.
func (mgr *BackendConnManager) ExecuteCmd(ctx context.Context, request []byte) (err error) {
	startTime := time.Now()
	mgr.processLock.Lock()
	defer func() {
		if err != nil && !pnet.IsMySQLError(err) {
			mgr.setQuitSourceByErr(err)
		}
		now := time.Now()
		if err != nil && errors.Is(err, ErrBackendConn) {
			cmd, data := pnet.Command(request[0]), request[1:]
			var query string
			if cmd == pnet.ComQuery {
				query = parser.Normalize(pnet.ParseQueryPacket(data))
				if len(query) > 256 {
					query = query[:256]
				}
			}
			// idle_time: maybe the idle time exceeds wait_timeout?
			// execute_time and query: maybe this query causes TiDB OOM?
			mgr.logger.Info("backend disconnects", zap.Duration("idle_time", now.Sub(mgr.lastActiveTime)),
				zap.Duration("execute_time", now.Sub(startTime)), zap.Stringer("cmd", cmd), zap.String("query", query))
		}
		mgr.lastActiveTime = now
		mgr.processLock.Unlock()
	}()
	if len(request) < 1 {
		err = mysql.ErrMalformPacket
		return
	}
	cmd := pnet.Command(request[0])

	// Once the request is accepted, it's treated in the transaction, so we don't check graceful shutdown here.
	if mgr.closeStatus.Load() >= statusClosing {
		return
	}
	backendIO := *mgr.backendIO.Load()
	err = mgr.cmdProcessor.executeCmd(request, mgr.clientIO, backendIO)
	if err != nil {
		if !pnet.IsMySQLError(err) {
			return
		} else {
			mgr.logger.Debug("got a mysql error", zap.Error(err), zap.Stringer("cmd", cmd))
		}
	}
	if err == nil {
		switch cmd {
		case pnet.ComQuit:
			return
		case pnet.ComSetOption:
			val := binary.LittleEndian.Uint16(request[1:])
			switch val {
			case 0:
				mgr.authenticator.capability |= pnet.ClientMultiStatements
				mgr.cmdProcessor.capability |= pnet.ClientMultiStatements
			case 1:
				mgr.authenticator.capability &^= pnet.ClientMultiStatements
				mgr.cmdProcessor.capability &^= pnet.ClientMultiStatements
			default:
				err = errors.Wrapf(mysql.ErrMalformPacket, "unrecognized set_option value:%d", val)
				return
			}
		case pnet.ComChangeUser:
			// Critical errors should not happen because CmdProcessor has parsed it already.
			req, _ := pnet.ParseChangeUser(request, mgr.authenticator.capability)
			mgr.authenticator.changeUser(req)
		}
	}
	// Even if it meets an MySQL error, it may have changed the status, such as when executing multi-statements.
	if mgr.cmdProcessor.finishedTxn() {
		if mgr.closeStatus.Load() == statusNotifyClose {
			mgr.tryGracefulClose(ctx)
		}
	}
	return
}

// SetEventReceiver implements SimpleConn.SetEventReceiver interface.
func (mgr *BackendConnManager) SetEventReceiver(receiver router.ConnEventReceiver) {
	mgr.eventReceiver.Store(&receiver)
}

func (mgr *BackendConnManager) getEventReceiver() router.ConnEventReceiver {
	eventReceiver := mgr.eventReceiver.Load()
	if eventReceiver == nil {
		return nil
	}
	return *eventReceiver
}

// processSignals runs in a goroutine to:
// - Check if the backend is still alive.
func (mgr *BackendConnManager) processSignals(ctx context.Context) {
	checkBackendTicker := time.NewTicker(mgr.config.TickerInterval)
	for {
		select {
		case s := <-mgr.signalReceived:
			func() {
				mgr.processLock.Lock()
				defer mgr.processLock.Unlock()
				switch s {
				case signalTypeGracefulClose:
					mgr.tryGracefulClose(ctx)
				}
			}()
		case <-checkBackendTicker.C:
			func() {
				mgr.checkBackendActive()
				mgr.processLock.Lock()
				defer mgr.processLock.Unlock()
				mgr.setKeepAlive()
			}()
		case <-ctx.Done():
			checkBackendTicker.Stop()
			return
		}
	}
}

// GracefulClose waits for the end of the transaction and closes the session.
func (mgr *BackendConnManager) GracefulClose() {
	if mgr.closeStatus.CompareAndSwap(statusActive, statusNotifyClose) {
		mgr.signalReceived <- signalTypeGracefulClose
	}
}

func (mgr *BackendConnManager) tryGracefulClose(ctx context.Context) {
	if mgr.closeStatus.Load() != statusNotifyClose || ctx.Err() != nil {
		return
	}
	if !mgr.cmdProcessor.finishedTxn() {
		return
	}
	mgr.quitSource = SrcProxyQuit
	// Closing clientIO will cause the whole connection to be closed.
	if err := mgr.clientIO.GracefulClose(); err != nil {
		mgr.logger.Warn("graceful close client IO error", zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
	}
	mgr.closeStatus.CompareAndSwap(statusNotifyClose, statusClosing)
}

func (mgr *BackendConnManager) checkBackendActive() {
	mgr.processLock.Lock()
	defer mgr.processLock.Unlock()

	if mgr.closeStatus.Load() >= statusNotifyClose {
		return
	}
	now := time.Now()
	if mgr.lastActiveTime.Add(mgr.config.CheckBackendInterval).After(now) {
		return
	}
	backendIO := *mgr.backendIO.Load()
	if !backendIO.IsPeerActive() {
		mgr.logger.Info("backend connection is closed, close client connection",
			zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Stringer("backend_addr", backendIO.RemoteAddr()),
			zap.Bool("backend_healthy", mgr.curBackend.Healthy()))
		mgr.quitSource = SrcBackendNetwork
		if err := mgr.clientIO.GracefulClose(); err != nil {
			mgr.logger.Warn("graceful close client IO error", zap.Stringer("client_addr", mgr.clientIO.RemoteAddr()), zap.Error(err))
		}
		mgr.closeStatus.CompareAndSwap(statusActive, statusClosing)
	} else {
		mgr.lastActiveTime = now
	}
}

func (mgr *BackendConnManager) ClientAddr() string {
	if mgr.clientIO == nil {
		return ""
	}
	return mgr.clientIO.RemoteAddr().String()
}

func (mgr *BackendConnManager) ServerAddr() string {
	if backendIO := mgr.backendIO.Load(); backendIO != nil {
		return (*backendIO).RemoteAddr().String()
	}
	return ""
}

func (mgr *BackendConnManager) ClientInBytes() uint64 {
	if mgr.clientIO == nil {
		return 0
	}
	return mgr.clientIO.InBytes()
}

func (mgr *BackendConnManager) ClientOutBytes() uint64 {
	if mgr.clientIO == nil {
		return 0
	}
	return mgr.clientIO.OutBytes()
}

func (mgr *BackendConnManager) QuitSource() ErrorSource {
	return mgr.quitSource
}

func (mgr *BackendConnManager) SetValue(key, val any) {
	mgr.ctxmap.Lock()
	mgr.ctxmap.m[key] = val
	mgr.ctxmap.Unlock()
}

func (mgr *BackendConnManager) Value(key any) any {
	mgr.ctxmap.Lock()
	v := mgr.ctxmap.m[key]
	mgr.ctxmap.Unlock()
	return v
}

// Close releases all resources.
func (mgr *BackendConnManager) Close() error {
	// BackendConnMgr may close even before connecting, so protect the members with a lock.
	mgr.processLock.Lock()
	defer func() {
		mgr.processLock.Unlock()
		// Wait out of the lock to avoid deadlock.
		mgr.wg.Wait()
	}()
	if mgr.closeStatus.Load() >= statusClosed {
		return nil
	}

	mgr.closeStatus.Store(statusClosing)
	if mgr.cancelFunc != nil {
		mgr.cancelFunc()
		mgr.cancelFunc = nil
	}

	// OnConnClose may read ServerAddr(), so call it before closing backendIO.
	handErr := mgr.handshakeHandler.OnConnClose(mgr, mgr.quitSource)

	var connErr error
	var addr string
	if backendIO := mgr.backendIO.Swap(nil); backendIO != nil {
		addr = (*backendIO).RemoteAddr().String()
		connErr = (*backendIO).Close()
	}

	eventReceiver := mgr.getEventReceiver()
	if eventReceiver != nil {
		// Notify the receiver if there's any event.
		if len(addr) > 0 {
			if err := eventReceiver.OnConnClosed(addr, mgr); err != nil {
				mgr.logger.Error("close connection error", zap.String("backend_addr", addr), zap.NamedError("notify_err", err))
			}
		}
	}
	mgr.closeStatus.Store(statusClosed)
	return errors.Collect(ErrCloseConnMgr, connErr, handErr)
}

// setKeepAlive sets keepalive on the backend connection based on the health status.
// NOTE: processLock should be held before calling this function.
func (mgr *BackendConnManager) setKeepAlive() {
	if mgr.closeStatus.Load() >= statusClosing {
		return
	}
	backendIO := mgr.backendIO.Load()
	if backendIO == nil {
		return
	}

	// The request to the unhealthy backend may block, instead of fail immediately.
	// So we set a shorter keep alive timeout for the unhealthy backends.
	cfg := mgr.config.HealthyKeepAlive
	curHealthy := mgr.curBackend.Healthy()
	if !curHealthy {
		cfg = mgr.config.UnhealthyKeepAlive
	}
	if err := (*backendIO).SetKeepalive(cfg); err != nil {
		mgr.logger.Warn("failed to set keepalive", zap.Stringer("backend_addr", (*backendIO).RemoteAddr()),
			zap.Bool("backend_healthy", curHealthy), zap.Error(err))
	}
}

func (mgr *BackendConnManager) setQuitSourceByErr(err error) {
	if err == nil {
		return
	}
	// The source may be already be set.
	// E.g. quitSource is set before TiProxy shuts down and client connection error is caused by shutdown instead of network.
	if mgr.quitSource != SrcNone {
		return
	}
	mgr.quitSource = Error2Source(err)
}

// UpdateLogger add fields to the logger.
// Note: it should be called within the lock.
func (mgr *BackendConnManager) UpdateLogger(fields ...zap.Field) {
	mgr.logger = mgr.logger.With(fields...)
}

// ConnInfo returns detailed info of the connection, which should not be logged too many times.
// Be careful about deadlocks.
func (mgr *BackendConnManager) ConnInfo() []zap.Field {
	mgr.processLock.Lock()
	var fields []zap.Field
	if mgr.authenticator != nil {
		fields = mgr.authenticator.ConnInfo()
	}
	mgr.processLock.Unlock()
	fields = append(fields, zap.String("backend_addr", mgr.ServerAddr()),
		zap.Time("create_time", mgr.createTime),
		zap.Time("last_active_time", mgr.lastActiveTime))
	return fields
}
