// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package logger

import (
	"github.com/mengchengtech/cerberus/pkg/config"
	lg "github.com/mengchengtech/cerberus/pkg/util/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerManager updates log configurations online.
type LoggerManager struct {
	// The logger used by LoggerManager itself to log.
	logger *zap.Logger
	syncer *lg.AtomicWriteSyncer
	level  zap.AtomicLevel
}

// NewLoggerManager creates a new LoggerManager.
func NewLoggerManager(cfg *config.Log) (*LoggerManager, *zap.Logger, error) {
	lm := &LoggerManager{}
	var err error
	mainLogger, syncer, level, err := lg.BuildLogger(cfg)
	if err != nil {
		return nil, nil, err
	}
	lm.syncer = syncer
	lm.level = level
	mainLogger = mainLogger.Named("main")
	lm.logger = mainLogger.Named("lgmgr")
	return lm, mainLogger, nil
}

func (lm *LoggerManager) SetLoggerLevel(l zapcore.Level) {
	lm.level.SetLevel(l)
}
