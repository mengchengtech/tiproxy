// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"

	"github.com/BurntSushi/toml"
	"github.com/mengchengtech/cerberus/pkg/config"
	"github.com/mengchengtech/cerberus/pkg/util/errors"
)

type configManager struct {
	config *config.Config
}

type ConfigManager interface {
	GetConfig() *config.Config
}

func NewConfigManager() *configManager {
	return &configManager{}
}

func (e *configManager) Init(configFile string) error {
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

func (e *configManager) reloadConfigFile(file string) error {
	content, err := os.ReadFile(file)
	if err != nil {
		return errors.WithStack(err)
	}

	return e.SetTOMLConfig(content)
}

// SetTOMLConfig will do partial config update. Usually, user will expect config changes
// only when they specified a config item. It is, however, impossible to tell a struct
// `c.max-conns == 0` means no user-input, or it specified `0`.
// So we always update the current config with a TOML string, which only overwrite fields
// that are specified by users.
func (e *configManager) SetTOMLConfig(data []byte) (err error) {
	base := e.config
	if base == nil {
		base = config.NewConfig()
	} else {
		base = base.Clone()
	}

	if err = toml.Unmarshal(data, base); err != nil {
		return errors.WithStack(err)
	}

	if err = base.Check(); err != nil {
		return
	}

	e.config = base
	return
}

func (e *configManager) GetConfig() *config.Config {
	return e.config
}
