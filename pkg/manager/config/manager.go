// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"time"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/tidwall/btree"
	"go.uber.org/zap"
)

const (
	pathPrefixNamespace = "ns"
)

const (
	checkFileInterval = 2 * time.Second
)

var (
	ErrNoResults = errors.Errorf("has no results")
)

type KVValue struct {
	Key   string
	Value []byte
}

type ConfigManager struct {
	logger        *zap.Logger
	advertiseAddr string

	kv *btree.BTreeG[KVValue]

	checkFileInterval time.Duration
	fileContent       []byte // used to compare whether the config file has changed
	current           *config.Config
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		checkFileInterval: checkFileInterval,
	}
}

func (e *ConfigManager) Init(ctx context.Context, logger *zap.Logger, configFile string, advertiseAddr string) error {
	e.logger = logger
	e.advertiseAddr = advertiseAddr

	// for namespace persistence
	e.kv = btree.NewBTreeG(func(a, b KVValue) bool {
		return a.Key < b.Key
	})

	if configFile != "" {
		if err := e.reloadConfigFile(configFile); err != nil {
			return err
		}
	} else {
		if err := e.SetTOMLConfig(nil); err != nil {
			return err
		}
	}

	return nil
}
