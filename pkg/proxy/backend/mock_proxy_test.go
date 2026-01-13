// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/mengchengtech/cerberus/lib/util/logger"
	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"go.uber.org/zap"
)

type proxyConfig struct {
	frontendTLSConfig *tls.Config
	backendTLSConfig  *tls.Config
	handler           *CustomHandshakeHandler
	bcConfig          *BCConfig
	username          string
	password          string
	sessionToken      string
	capability        pnet.Capability
	connectionID      uint64
}

func newProxyConfig() *proxyConfig {
	return &proxyConfig{
		handler:      &CustomHandshakeHandler{},
		capability:   defaultTestBackendCapability,
		sessionToken: mockToken,
		bcConfig:     &BCConfig{},
	}
}

type mockProxy struct {
	*BackendConnManager

	*proxyConfig
	// outputs that received from the server.
	rs *mysql.Resultset
	// execution results
	err    error
	logger *zap.Logger
	text   fmt.Stringer
}

func newMockProxy(t *testing.T, cfg *proxyConfig) *mockProxy {
	lg, text := logger.CreateLoggerForTest(t)
	mp := &mockProxy{
		proxyConfig:        cfg,
		logger:             lg.Named("mockProxy"),
		text:               text,
		BackendConnManager: NewBackendConnManager(lg, cfg.handler, cfg.connectionID, cfg.bcConfig),
	}
	mp.cmdProcessor.capability = cfg.capability
	return mp
}

func (mp *mockProxy) authenticateFirstTime(clientIO, backendIO pnet.PacketIO) error {
	if err := mp.authenticator.handshakeFirstTime(context.Background(), mp.logger, mp, clientIO, mp.handshakeHandler,
		func(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		}, mp.frontendTLSConfig, mp.backendTLSConfig); err != nil {
		return err
	}
	mp.cmdProcessor.capability = mp.authenticator.capability
	return nil
}

func (mp *mockProxy) authenticateSecondTime(clientIO, backendIO pnet.PacketIO) error {
	return mp.authenticator.handshakeSecondTime(mp.logger, clientIO, backendIO, mp.backendTLSConfig, mp.sessionToken)
}

func (mp *mockProxy) authenticateWithBackend(_, backendIO pnet.PacketIO) error {
	if err := mp.authenticator.handshakeWithBackend(context.Background(), mp.logger, mp, mp.handshakeHandler,
		mp.username, mp.password, func(ctx context.Context, cctx ConnContext, resp *pnet.HandshakeResp) (pnet.PacketIO, error) {
			return backendIO, nil
		}, mp.backendTLSConfig); err != nil {
		return err
	}
	mp.cmdProcessor.capability = mp.authenticator.capability
	return nil
}

func (mp *mockProxy) processCmd(clientIO, backendIO pnet.PacketIO) error {
	clientIO.ResetSequence()
	request, err := clientIO.ReadPacket()
	if err != nil {
		return err
	}
	if err = mp.cmdProcessor.executeCmd(request, clientIO, backendIO); err != nil {
		return err
	}
	return err
}

func (mp *mockProxy) directQuery(_, backendIO pnet.PacketIO) error {
	rs, _, err := mp.cmdProcessor.query(backendIO, mockCmdStr)
	mp.rs = rs
	return err
}
