// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package proxy

import "github.com/mengchengtech/cerberus/pkg/util/errors"

var (
	ErrCloseServer = errors.New("failed to close sqlserver")
)
