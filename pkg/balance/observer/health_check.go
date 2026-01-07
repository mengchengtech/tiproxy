// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	"context"
	"net"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/mengchengtech/cerberus/pkg/config"
	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"github.com/mengchengtech/cerberus/pkg/util/errors"
	"go.uber.org/zap"
)

// HealthCheck is used to check the backends of one backend. One can pass a customized health check function to the observer.
type HealthCheck interface {
	Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth
}

type DefaultHealthCheck struct {
	cfg    *config.HealthCheck
	logger *zap.Logger
}

func NewDefaultHealthCheck(cfg *config.HealthCheck, logger *zap.Logger) *DefaultHealthCheck {
	return &DefaultHealthCheck{
		cfg:    cfg,
		logger: logger,
	}
}

func (dhc *DefaultHealthCheck) Check(ctx context.Context, addr string, info *BackendInfo) *BackendHealth {
	bh := &BackendHealth{
		BackendInfo: *info,
		Healthy:     true,
	}
	if !dhc.cfg.Enable {
		return bh
	}
	if !bh.Healthy {
		return bh
	}
	dhc.checkSqlPort(ctx, addr, bh)
	return bh
}

func (dhc *DefaultHealthCheck) checkSqlPort(ctx context.Context, addr string, bh *BackendHealth) {
	// Also dial the SQL port just in case that the SQL port hangs.
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(dhc.cfg.RetryInterval), uint64(dhc.cfg.MaxRetries)), ctx)
	err := connectWithRetry(func() error {
		conn, err := net.DialTimeout("tcp", addr, dhc.cfg.DialTimeout)
		if err != nil {
			return err
		}
		if err = conn.SetReadDeadline(time.Now().Add(dhc.cfg.DialTimeout)); err != nil {
			return err
		}
		if err = pnet.CheckSqlPort(conn); err != nil {
			return err
		}
		if ignoredErr := conn.Close(); ignoredErr != nil && !pnet.IsDisconnectError(ignoredErr) {
			dhc.logger.Warn("close connection in health check failed", zap.Error(ignoredErr))
		}
		return err
	}, b)
	if err != nil {
		bh.Healthy = false
		bh.PingErr = errors.Wrapf(err, "connect sql port failed")
	}
}

func connectWithRetry(connect func() error, b backoff.BackOff) error {
	err := backoff.Retry(func() error {
		err := connect()
		if !pnet.IsRetryableError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
	return err
}
