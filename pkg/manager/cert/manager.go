// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cert

import (
	"crypto/tls"
	"sync/atomic"
	"time"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/lib/util/security"
	"go.uber.org/zap"
)

const (
	defaultRetryInterval = 1 * time.Hour
)

// CertManager reloads certs and offers interfaces for fetching TLS configs.
// Currently, all the namespaces share the same certs but there might be per-namespace
// certs in the future.
type CertManager struct {
	serverSQLTLS       *security.CertInfo // client -> proxy
	serverSQLTLSConfig atomic.Pointer[tls.Config]
	sqlTLS             *security.CertInfo // proxy -> tidb sql port
	sqlTLSConfig       atomic.Pointer[tls.Config]

	retryInterval atomic.Int64
	logger        *zap.Logger
}

// NewCertManager creates a new CertManager.
func NewCertManager() *CertManager {
	cm := &CertManager{}
	cm.SetRetryInterval(defaultRetryInterval)
	return cm
}

// Init creates a CertManager and reloads certificates periodically.
// cfgch can be set to nil for the serverless tier because it has no config manager.
func (cm *CertManager) Init(cfg *config.Config, logger *zap.Logger) error {
	cm.logger = logger
	cm.serverSQLTLS = security.NewCert(true)
	cm.sqlTLS = security.NewCert(false)
	cm.setConfig(cfg)
	if err := cm.reload(); err != nil {
		return err
	}

	return nil
}

func (cm *CertManager) setConfig(cfg *config.Config) {
	cm.serverSQLTLS.SetConfig(cfg.Security.ServerSQLTLS)
	cm.sqlTLS.SetConfig(cfg.Security.SQLTLS)
}

func (cm *CertManager) SetRetryInterval(interval time.Duration) {
	cm.retryInterval.Store(int64(interval))
}

func (cm *CertManager) ServerSQLTLS() *tls.Config {
	return cm.serverSQLTLSConfig.Load()
}

func (cm *CertManager) SQLTLS() *tls.Config {
	return cm.sqlTLSConfig.Load()
}

// If any error happens, we still continue and use the old cert.
func (cm *CertManager) reload() error {
	errs := make([]error, 0, 4)
	if tlsConfig, err := cm.serverSQLTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.serverSQLTLSConfig.Store(tlsConfig)
	}
	if tlsConfig, err := cm.sqlTLS.Reload(cm.logger); err != nil {
		errs = append(errs, err)
	} else {
		cm.sqlTLSConfig.Store(tlsConfig)
	}
	var err error
	if len(errs) > 0 {
		err = errors.Collect(errors.New("loading certs"), errs...)
		cm.logger.Error("failed to reload some certs", zap.Error(err))
	}
	return err
}
