// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"context"

	"github.com/mengchengtech/cerberus/pkg/balance/factor"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/router"
	mconfig "github.com/mengchengtech/cerberus/pkg/manager/config"
	"go.uber.org/zap"
)

type RouterManager interface {
	GetRouter() router.Router
	Init(logger *zap.Logger, cfgMgr *mconfig.ConfigManager) error
}

type routerManager struct {
	bo     observer.BackendObserver
	router router.Router
}

func (n *routerManager) GetRouter() router.Router {
	return n.router
}

func NewRouterManager() RouterManager {
	return &routerManager{}
}

func (n *routerManager) Init(logger *zap.Logger, cfgMgr *mconfig.ConfigManager) error {
	cfg := cfgMgr.GetConfig()
	// init BackendFetcher
	fetcher := observer.NewStaticFetcher(cfg.Proxy.Backend.Instances)

	// init Router
	cf := &cfg.Proxy.Backend.Health
	rt := router.NewScoreBasedRouter(logger.Named("router"))
	hc := observer.NewDefaultHealthCheck(cf, logger.Named("hc"))
	bo := observer.NewDefaultBackendObserver(logger.Named("observer"), cf, fetcher, hc)
	bo.Start(context.Background())
	balancePolicy := factor.NewFactorBasedBalance(logger.Named("factor"))
	rt.Init(context.Background(), bo, balancePolicy, cfg)

	n.bo = bo
	n.router = rt
	return nil
}
