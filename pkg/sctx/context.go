// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sctx

import (
	"github.com/mengchengtech/cerberus/pkg/proxy/backend"
)

type Context struct {
	ConfigFile string
	Handler    backend.HandshakeHandler
}
