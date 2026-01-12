// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"runtime"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/mengchengtech/cerberus/pkg/manager/cert"
	mgrcfg "github.com/mengchengtech/cerberus/pkg/manager/config"
	"github.com/mengchengtech/cerberus/pkg/manager/id"
	"github.com/mengchengtech/cerberus/pkg/manager/logger"
	mgrns "github.com/mengchengtech/cerberus/pkg/manager/namespace"
	"github.com/mengchengtech/cerberus/pkg/proxy"
	"github.com/mengchengtech/cerberus/pkg/proxy/backend"
	"github.com/mengchengtech/cerberus/pkg/sctx"
	"github.com/mengchengtech/cerberus/pkg/util/etcd"
	"github.com/mengchengtech/cerberus/pkg/util/versioninfo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Server struct {
	wg waitgroup.WaitGroup
	// managers
	configManager    *mgrcfg.ConfigManager
	namespaceManager mgrns.NamespaceManager
	loggerManager    *logger.LoggerManager
	certManager      *cert.CertManager
	// etcd client
	etcdCli *clientv3.Client
	// L7 proxy
	proxy *proxy.SQLServer
}

func NewServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		configManager:    mgrcfg.NewConfigManager(),
		namespaceManager: mgrns.NewNamespaceManager(),
		certManager:      cert.NewCertManager(),
	}

	handler := sctx.Handler
	ready := atomic.NewBool(false)

	// set up logger
	var lg *zap.Logger
	if srv.loggerManager, lg, err = logger.NewLoggerManager(nil); err != nil {
		return
	}
	srv.loggerManager.Init(srv.configManager.WatchConfig())

	// setup config manager
	if err = srv.configManager.Init(ctx, lg.Named("config"), sctx.ConfigFile, sctx.AdvertiseAddr); err != nil {
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
	if err = srv.certManager.Init(cfg, lg.Named("cert"), srv.configManager.WatchConfig()); err != nil {
		return
	}

	// setup etcd client
	srv.etcdCli, err = etcd.InitEtcdClient(lg.Named("etcd"), cfg, srv.certManager)
	if err != nil {
		return
	}

	// setup namespace manager
	{
		nscs, nerr := srv.configManager.ListAllNamespace(ctx)
		if nerr != nil {
			err = nerr
			return
		}

		if len(nscs) == 0 {
			// no existed namespace
			nsc := &config.Namespace{
				Namespace: "default",
				Backend: config.BackendNamespace{
					Instances: []string{},
				},
			}
			if err = srv.configManager.SetNamespace(ctx, nsc.Namespace, nsc); err != nil {
				return
			}
			nscs = append(nscs, nsc)
		}

		err = srv.namespaceManager.Init(lg.Named("nsmgr"), nscs, srv.configManager)
		if err != nil {
			return
		}
	}

	var hsHandler backend.HandshakeHandler
	if handler != nil {
		hsHandler = handler
	} else {
		hsHandler = backend.NewDefaultHandshakeHandler(srv.namespaceManager)
	}

	// setup proxy server
	idMgr := id.NewIDManager()
	{
		srv.proxy, err = proxy.NewSQLServer(lg.Named("proxy"), cfg, srv.certManager, idMgr, hsHandler)
		if err != nil {
			return
		}
		srv.proxy.Run(ctx, srv.configManager.WatchConfig())
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
	if s.namespaceManager != nil {
		errs = append(errs, s.namespaceManager.Close())
	}
	if s.configManager != nil {
		errs = append(errs, s.configManager.Close())
	}
	if s.loggerManager != nil {
		errs = append(errs, s.loggerManager.Close())
	}
	if s.etcdCli != nil {
		errs = append(errs, s.etcdCli.Close())
	}
	s.wg.Wait()
	return errors.Collect(ErrCloseServer, errs...)
}
