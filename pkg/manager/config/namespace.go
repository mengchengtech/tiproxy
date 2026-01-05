// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"encoding/json"
	"path"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/errors"
)

func (e *ConfigManager) get(ctx context.Context, ns, key string) (KVValue, error) {
	nkey := path.Clean(path.Join(ns, key))
	v, ok := e.kv.Get(KVValue{Key: nkey})
	if !ok {
		return v, errors.WithStack(errors.Wrapf(ErrNoResults, "key=%s", nkey))
	}
	return v, nil
}

func (e *ConfigManager) set(ctx context.Context, ns, key string, val []byte) error {
	v := KVValue{Key: path.Clean(path.Join(ns, key)), Value: val}
	_, _ = e.kv.Set(v)
	return nil
}

func (e *ConfigManager) GetNamespace(ctx context.Context, ns string) (*config.Namespace, error) {
	kv, err := e.get(ctx, pathPrefixNamespace, ns)
	if err != nil {
		return nil, err
	}
	var cfg config.Namespace
	err = json.Unmarshal(kv.Value, &cfg)
	return &cfg, err
}

func (e *ConfigManager) SetNamespace(ctx context.Context, ns string, nsc *config.Namespace) error {
	if ns == "" || nsc.Namespace == "" {
		return errors.New("namespace name can not be empty string")
	}
	r, err := json.Marshal(nsc)
	if err != nil {
		return err
	}
	return e.set(ctx, pathPrefixNamespace, ns, r)
}
