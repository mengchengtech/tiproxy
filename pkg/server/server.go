// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"runtime"

	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/mengchengtech/cerberus/pkg/manager/cert"
	mgrcfg "github.com/mengchengtech/cerberus/pkg/manager/config"
	"github.com/mengchengtech/cerberus/pkg/manager/id"
	"github.com/mengchengtech/cerberus/pkg/manager/logger"
	mgrrouter "github.com/mengchengtech/cerberus/pkg/manager/router"
	"github.com/mengchengtech/cerberus/pkg/proxy"
	"github.com/mengchengtech/cerberus/pkg/proxy/backend"
	"github.com/mengchengtech/cerberus/pkg/sctx"
	"github.com/mengchengtech/cerberus/pkg/util/versioninfo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	wg waitgroup.WaitGroup
	// managers
	configManager *mgrcfg.ConfigManager
	routerManager mgrrouter.RouterManager
	loggerManager *logger.LoggerManager
	certManager   *cert.CertManager
	// L7 proxy
	proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		configManager: mgrcfg.NewConfigManager(),
		routerManager: mgrrouter.NewRouterManager(),
		certManager:   cert.NewCertManager(),
	}

	handler := sctx.Handler
	ready := atomic.NewBool(false)

	// set up logger
	var lg *zap.Logger
	if srv.loggerManager, lg, err = logger.NewLoggerManager(nil); err != nil {
		return
	}

	// setup config manager
	if err = srv.configManager.Init(ctx, lg.Named("config"), sctx.ConfigFile); err != nil {
		return
	}
	cfg := srv.configManager.GetConfig()

	// welcome messages must be printed after initialization of configmager, because
	// logfile backended zaplogger is enabled after cfgmgr.Init(..).
	// otherwise, printInfo will output to stdout, which can not be redirected to the log file on tiup-cluster.
	//
	// TODO: there is a race condition that printInfo and logmgr may concurrently execute:
	// logmgr may havenot been initialized with logfile yet
	// Make sure the TiProxy info is always printed.
	level := lg.Level()
	srv.loggerManager.SetLoggerLevel(zap.InfoLevel)
	printInfo(lg)
	srv.loggerManager.SetLoggerLevel(level)

	// setup certs
	if err = srv.certManager.Init(cfg, lg.Named("cert")); err != nil {
		return
	}

	// setup namespace manager
	{
		err = srv.routerManager.Init(lg.Named("nsmgr"), srv.configManager)
		if err != nil {
			return
		}
	}

	var hsHandler backend.HandshakeHandler
	if handler != nil {
		hsHandler = handler
	} else {
		hsHandler = backend.NewDefaultHandshakeHandler(srv.routerManager)
	}

	// setup proxy server
	idMgr := id.NewIDManager()
	{
		srv.proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg, srv.certManager, idMgr, hsHandler)
		if err != nil {
			return
		}
		srv.proxy.Run(ctx)
	}

	ready.Toggle()
	return
}

func printInfo(lg *zap.Logger) {
	fields := []zap.Field{
		zap.String("Release Version", versioninfo.TiProxyVersion),
		zap.String("Git Commit Hash", versioninfo.TiProxyGitHash),
		zap.String("Git Branch", versioninfo.TiProxyGitBranch),
		zap.String("UTC Build Time", versioninfo.TiProxyBuildTS),
		zap.String("GoVersion", runtime.Version()),
		zap.String("OS", runtime.GOOS),
		zap.String("Arch", runtime.GOARCH),
	}
	lg.Info("Welcome to TiProxy.", fields...)
}

func (s *Server) preClose() {
	// Gracefully drain clients.
	if s.proxy != nil {
		s.proxy.PreClose()
	}
}

func (s *Server) Close() error {
	s.preClose()

	errs := make([]error, 0, 4)
	if s.proxy != nil {
		errs = append(errs, s.proxy.Close())
	}
	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
