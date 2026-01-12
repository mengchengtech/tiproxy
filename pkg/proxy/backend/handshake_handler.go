// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/pkg/balance/router"
	routermgr "github.com/mengchengtech/cerberus/pkg/manager/router"
	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"go.uber.org/zap"
)

// Interfaces in this file are used for the serverless tier.

// Context keys.
type ConnContextKey string

const (
	ConnContextKeyTLSState ConnContextKey = "tls-state"
	ConnContextKeyConnID   ConnContextKey = "conn-id"
	ConnContextKeyConnAddr ConnContextKey = "conn-addr"
)

var _ HandshakeHandler = (*DefaultHandshakeHandler)(nil)
var _ HandshakeHandler = (*CustomHandshakeHandler)(nil)

// ConnContext saves the connection attributes that are read by HandshakeHandler.
// These interfaces should not request for locks because HandshakeHandler already holds the lock.
type ConnContext interface {
	ClientAddr() string
	ServerAddr() string
	ClientInBytes() uint64
	ClientOutBytes() uint64
	UpdateLogger(fields ...zap.Field)
	SetValue(key, val any)
	Value(key any) any
}

// HandshakeHandler contains the hooks that are called during the connection lifecycle.
// All the interfaces should be called within a lock so that the interfaces of ConnContext are thread-safe.
type HandshakeHandler interface {
	HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error
	HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool // return true means retry connect
	GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	OnHandshake(ctx ConnContext, to string, err error, src ErrorSource)
	OnConnClose(ctx ConnContext, src ErrorSource) error
	GetCapability() pnet.Capability
	GetServerVersion() string
}

type DefaultHandshakeHandler struct {
	routerManager routermgr.RouterManager
}

func NewDefaultHandshakeHandler(routerManager routermgr.RouterManager) *DefaultHandshakeHandler {
	return &DefaultHandshakeHandler{
		routerManager: routerManager,
	}
}

func (handler *DefaultHandshakeHandler) HandleHandshakeResp(ConnContext, *pnet.HandshakeResp) error {
	return nil
}

func (handler *DefaultHandshakeHandler) HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool {
	return false
}

func (handler *DefaultHandshakeHandler) GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	return handler.routerManager.GetRouter(), nil
}

func (handler *DefaultHandshakeHandler) OnHandshake(ConnContext, string, error, ErrorSource) {
}

func (handler *DefaultHandshakeHandler) OnConnClose(ConnContext, ErrorSource) error {
	return nil
}

func (handler *DefaultHandshakeHandler) GetCapability() pnet.Capability {
	return SupportedServerCapabilities
}

func (handler *DefaultHandshakeHandler) GetServerVersion() string {
	return pnet.ServerVersion
}

type CustomHandshakeHandler struct {
	getRouter           func(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error)
	onHandshake         func(ConnContext, string, error, ErrorSource)
	onConnClose         func(ConnContext, ErrorSource) error
	handleHandshakeResp func(ctx ConnContext, resp *pnet.HandshakeResp) error
	handleHandshakeErr  func(ctx ConnContext, err *mysql.MyError) bool
	getCapability       func() pnet.Capability
	getServerVersion    func() string
}

func (h *CustomHandshakeHandler) GetRouter(ctx ConnContext, resp *pnet.HandshakeResp) (router.Router, error) {
	if h.getRouter != nil {
		return h.getRouter(ctx, resp)
	}
	return nil, errors.New("no router")
}

func (h *CustomHandshakeHandler) OnHandshake(ctx ConnContext, addr string, err error, src ErrorSource) {
	if h.onHandshake != nil {
		h.onHandshake(ctx, addr, err, src)
	}
}

func (h *CustomHandshakeHandler) OnConnClose(ctx ConnContext, src ErrorSource) error {
	if h.onConnClose != nil {
		return h.onConnClose(ctx, src)
	}
	return nil
}

func (h *CustomHandshakeHandler) HandleHandshakeResp(ctx ConnContext, resp *pnet.HandshakeResp) error {
	if h.handleHandshakeResp != nil {
		return h.handleHandshakeResp(ctx, resp)
	}
	return nil
}

func (h *CustomHandshakeHandler) HandleHandshakeErr(ctx ConnContext, err *mysql.MyError) bool {
	if h.handleHandshakeErr != nil {
		return h.handleHandshakeErr(ctx, err)
	}
	return false
}

func (h *CustomHandshakeHandler) GetCapability() pnet.Capability {
	if h.getCapability != nil {
		return h.getCapability()
	}
	return SupportedServerCapabilities
}

func (h *CustomHandshakeHandler) GetServerVersion() string {
	if h.getServerVersion != nil {
		return h.getServerVersion()
	}
	return pnet.ServerVersion
}
