// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
)

func (e *ConfigManager) reloadConfigFile(file string) error {
	content, err := os.ReadFile(file)
	if err != nil {
		return errors.WithStack(err)
	}
	if bytes.Equal(content, e.fileContent) {
		return nil
	}
	e.fileContent = content

	return e.SetTOMLConfig(content)
}

// SetTOMLConfig will do partial config update. Usually, user will expect config changes
// only when they specified a config item. It is, however, impossible to tell a struct
// `c.max-conns == 0` means no user-input, or it specified `0`.
// So we always update the current config with a TOML string, which only overwrite fields
// that are specified by users.
func (e *ConfigManager) SetTOMLConfig(data []byte) (err error) {
	base := e.current
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

	e.current = base
	return
}

func (e *ConfigManager) GetConfig() *config.Config {
	return e.current
}
