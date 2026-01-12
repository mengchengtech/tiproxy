// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/pkg/balance/factor"
	"github.com/mengchengtech/cerberus/pkg/balance/observer"
	"github.com/mengchengtech/cerberus/pkg/balance/router"
	mconfig "github.com/mengchengtech/cerberus/pkg/manager/config"
	"github.com/mengchengtech/cerberus/pkg/util/http"
	"go.uber.org/zap"
)

type NamespaceManager interface {
	Init(logger *zap.Logger, nscs []*config.Namespace, tpFetcher observer.TopologyFetcher,
		httpCli *http.Client, cfgMgr *mconfig.ConfigManager) error
	CommitNamespaces(nss []*config.Namespace, nssDelete []bool) error
	GetNamespace(nm string) (*Namespace, bool)
	GetNamespaceByUser(user string) (*Namespace, bool)
	Ready() bool
	Close() error
}

type namespaceManager struct {
	sync.RWMutex
	nsm       map[string]*Namespace
	tpFetcher observer.TopologyFetcher
	httpCli   *http.Client
	logger    *zap.Logger
	cfgMgr    *mconfig.ConfigManager
}

func NewNamespaceManager() *namespaceManager {
	return &namespaceManager{}
}

func (mgr *namespaceManager) buildNamespace(cfg *config.Namespace) (*Namespace, error) {
	logger := mgr.logger.With(zap.String("namespace", cfg.Namespace))

	// init BackendFetcher
	var fetcher observer.BackendFetcher
	healthCheckCfg := config.NewDefaultHealthCheckConfig()
	if mgr.tpFetcher != nil && !reflect.ValueOf(mgr.tpFetcher).IsNil() {
		fetcher = observer.NewPDFetcher(mgr.tpFetcher, logger.Named("be_fetcher"), healthCheckCfg)
	} else {
		fetcher = observer.NewStaticFetcher(cfg.Backend.Instances)
	}

	// init Router
	rt := router.NewScoreBasedRouter(logger.Named("router"))
	hc := observer.NewDefaultHealthCheck(mgr.httpCli, healthCheckCfg, logger.Named("hc"))
	bo := observer.NewDefaultBackendObserver(logger.Named("observer"), healthCheckCfg, fetcher, hc)
	bo.Start(context.Background())
	balancePolicy := factor.NewFactorBasedBalance(logger.Named("factor"))
	rt.Init(context.Background(), bo, balancePolicy, mgr.cfgMgr.GetConfig(), mgr.cfgMgr.WatchConfig())

	return &Namespace{
		name:   cfg.Namespace,
		user:   cfg.Frontend.User,
		bo:     bo,
		router: rt,
	}, nil
}

func (mgr *namespaceManager) CommitNamespaces(nss []*config.Namespace, nssDelete []bool) error {
	nsm := make(map[string]*Namespace)
	mgr.RLock()
	for k, v := range mgr.nsm {
		nsm[k] = v
	}
	mgr.RUnlock()

	for i, nsc := range nss {
		if nssDelete != nil && nssDelete[i] {
			delete(nsm, nsc.Namespace)
			continue
		}

		ns, err := mgr.buildNamespace(nsc)
		if err != nil {
			return fmt.Errorf("%w: create namespace error, namespace: %s", err, nsc.Namespace)
		}
		nsm[ns.Name()] = ns
	}

	mgr.Lock()
	mgr.nsm = nsm
	mgr.Unlock()
	return nil
}

func (mgr *namespaceManager) Init(logger *zap.Logger, nscs []*config.Namespace, tpFetcher observer.TopologyFetcher,
	httpCli *http.Client, cfgMgr *mconfig.ConfigManager) error {
	mgr.Lock()
	mgr.tpFetcher = tpFetcher
	mgr.httpCli = httpCli
	mgr.logger = logger
	mgr.cfgMgr = cfgMgr
	mgr.Unlock()
	return mgr.CommitNamespaces(nscs, nil)
}

func (mgr *namespaceManager) GetNamespace(nm string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	ns, ok := mgr.nsm[nm]
	return ns, ok
}

func (mgr *namespaceManager) GetNamespaceByUser(user string) (*Namespace, bool) {
	mgr.RLock()
	defer mgr.RUnlock()

	for _, ns := range mgr.nsm {
		if ns.User() == user {
			return ns, true
		}
	}
	return nil, false
}

func (mgr *namespaceManager) Ready() bool {
	mgr.RLock()
	defer mgr.RUnlock()
	if len(mgr.nsm) == 0 {
		return false
	}
	for _, ns := range mgr.nsm {
		if ns.GetRouter().HealthyBackendCount() <= 0 {
			return false
		}
	}
	return true
}

func (mgr *namespaceManager) Close() error {
	mgr.RLock()
	for _, ns := range mgr.nsm {
		ns.Close()
	}
	mgr.RUnlock()
	return nil
}
