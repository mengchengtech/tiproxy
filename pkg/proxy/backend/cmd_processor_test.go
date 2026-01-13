// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"testing"

	pnet "github.com/mengchengtech/cerberus/pkg/proxy/net"
	"github.com/stretchr/testify/require"
)

type respondType int

const (
	responseTypeNone respondType = iota
	responseTypeOK
	responseTypeErr
	responseTypeResultSet
	responseTypeColumn
	responseTypeLoadFile
	responseTypeString
	responseTypeEOF
	responseTypeSwitchRequest
	responseTypePrepareOK
	responseTypeRow
)

// cmdResponseTypes lists all commands and their responses.
var cmdResponseTypes = map[pnet.Command][]respondType{
	pnet.ComSleep:     {responseTypeErr},
	pnet.ComQuit:      {responseTypeNone},
	pnet.ComInitDB:    {responseTypeOK, responseTypeErr},
	pnet.ComQuery:     {responseTypeOK, responseTypeErr, responseTypeResultSet, responseTypeLoadFile},
	pnet.ComFieldList: {responseTypeErr, responseTypeColumn},
	pnet.ComCreateDB:  {responseTypeOK, responseTypeErr},
	pnet.ComDropDB:    {responseTypeOK, responseTypeErr},
	pnet.ComRefresh:   {responseTypeOK, responseTypeErr},
	// It is comShutdown
	pnet.ComDeprecated1:      {responseTypeOK, responseTypeErr},
	pnet.ComStatistics:       {responseTypeString},
	pnet.ComProcessInfo:      {responseTypeErr, responseTypeResultSet},
	pnet.ComConnect:          {responseTypeErr},
	pnet.ComProcessKill:      {responseTypeOK, responseTypeErr},
	pnet.ComDebug:            {responseTypeEOF, responseTypeErr},
	pnet.ComPing:             {responseTypeOK},
	pnet.ComTime:             {responseTypeErr},
	pnet.ComDelayedInsert:    {responseTypeErr},
	pnet.ComChangeUser:       {responseTypeSwitchRequest, responseTypeOK, responseTypeErr},
	pnet.ComBinlogDump:       {responseTypeErr},
	pnet.ComTableDump:        {responseTypeErr},
	pnet.ComConnectOut:       {responseTypeErr},
	pnet.ComRegisterSlave:    {responseTypeErr},
	pnet.ComStmtPrepare:      {responseTypePrepareOK, responseTypeErr},
	pnet.ComStmtExecute:      {responseTypeOK, responseTypeErr, responseTypeResultSet},
	pnet.ComStmtSendLongData: {responseTypeNone},
	pnet.ComStmtClose:        {responseTypeNone},
	pnet.ComStmtReset:        {responseTypeOK, responseTypeErr},
	pnet.ComSetOption:        {responseTypeEOF, responseTypeErr},
	pnet.ComStmtFetch:        {responseTypeRow, responseTypeErr},
	pnet.ComDaemon:           {responseTypeErr},
	pnet.ComBinlogDumpGtid:   {responseTypeErr},
	pnet.ComResetConnection:  {responseTypeOK, responseTypeErr},
	pnet.ComEnd:              {responseTypeErr},
}

// Test forwarding packets between the client and the backend.
func TestForwardCommands(t *testing.T) {
	tc := newTCPConnSuite(t)
	runTest := func(cfgs ...cfgOverrider) {
		ts, clean := newTestSuite(t, tc, cfgs...)
		ts.executeCmd(t, nil)
		clean()
	}
	// Test every respond type for every command.
	for cmd, respondTypes := range cmdResponseTypes {
		for _, respondType := range respondTypes {
			for _, capability := range []pnet.Capability{defaultTestBackendCapability &^ pnet.ClientDeprecateEOF, defaultTestBackendCapability | pnet.ClientDeprecateEOF} {
				cfgOvr := func(cfg *testConfig) {
					cfg.clientConfig.cmd = cmd
					cfg.backendConfig.respondType = respondType
					cfg.backendConfig.capability = capability
					cfg.clientConfig.capability = capability
					cfg.proxyConfig.capability = capability
				}
				// Test more variables for some special response types.
				switch respondType {
				case responseTypeColumn:
					for _, columns := range []int{1, 4096} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.backendConfig.columns = columns
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				case responseTypeRow:
					for _, rows := range []int{0, 1, 3} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.backendConfig.rows = rows
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				case responseTypePrepareOK:
					for _, columns := range []int{0, 1, 4096} {
						for _, params := range []int{0, 1, 3} {
							extraCfgOvr := func(cfg *testConfig) {
								cfg.backendConfig.columns = columns
								cfg.backendConfig.params = params
							}
							runTest(cfgOvr, extraCfgOvr)
						}
					}
				case responseTypeResultSet:
					for _, columns := range []int{1, 4096} {
						for _, rows := range []int{0, 1, 3} {
							for _, status := range []uint16{0, pnet.ServerStatusCursorExists} {
								extraCfgOvr := func(cfg *testConfig) {
									cfg.backendConfig.columns = columns
									cfg.backendConfig.rows = rows
									cfg.backendConfig.status = status
								}
								runTest(cfgOvr, extraCfgOvr)
							}
						}
					}
				case responseTypeLoadFile:
					for _, filePkts := range []int{0, 1, 3} {
						extraCfgOvr := func(cfg *testConfig) {
							cfg.clientConfig.filePkts = filePkts
						}
						runTest(cfgOvr, extraCfgOvr)
					}
				default:
					runTest(cfgOvr, cfgOvr)
				}
			}
		}
	}
}

// Test querying directly from the server.
func TestDirectQuery(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfg cfgOverrider
		c   checker
	}{
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.columns = 2
				cfg.backendConfig.rows = 1
				cfg.backendConfig.respondType = responseTypeResultSet
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.clientConfig.capability = defaultTestBackendCapability &^ pnet.ClientDeprecateEOF
				cfg.proxyConfig.capability = cfg.clientConfig.capability
				cfg.backendConfig.capability = cfg.clientConfig.capability
				cfg.backendConfig.columns = 2
				cfg.backendConfig.rows = 1
				cfg.backendConfig.respondType = responseTypeResultSet
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeErr
			},
			c: func(t *testing.T, ts *testSuite) {
				require.Error(t, ts.mp.err)
				require.Equal(t, SrcClientSQLErr, Error2Source(ts.mp.err))
				require.NoError(t, ts.mb.err)
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeOK
			},
		},
	}
	for _, test := range tests {
		ts, clean := newTestSuite(t, tc, test.cfg)
		ts.query(t, test.c)
		clean()
	}
}

func TestPreparedStmts(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfgs []cfgOverrider
	}{
		// prepare
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtPrepare
					cfg.backendConfig.respondType = responseTypePrepareOK
				},
			},
		},
		// send long data
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
		},
		// send long data and execute
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeErr
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
			},
		},
		// execute and fetch
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = pnet.ServerStatusLastRowSend
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtFetch
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeErr
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtFetch
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = pnet.ServerStatusLastRowSend
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtFetch
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeRow
					cfg.backendConfig.status = pnet.ServerStatusLastRowSend
				},
			},
		},
		// send long data and close/reset
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtClose
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtClose
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtReset
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtReset
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		// execute and close/reset
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtClose
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtReset
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtClose
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeNone
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtReset
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		// reset connection and change user
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComResetConnection
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
		{
			cfgs: []cfgOverrider{
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtSendLongData
					cfg.clientConfig.prepStmtID = 1
					cfg.backendConfig.respondType = responseTypeNone
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComStmtExecute
					cfg.clientConfig.prepStmtID = 2
					cfg.backendConfig.columns = 1
					cfg.backendConfig.respondType = responseTypeResultSet
					cfg.backendConfig.status = pnet.ServerStatusCursorExists
				},
				func(cfg *testConfig) {
					cfg.clientConfig.cmd = pnet.ComChangeUser
					cfg.backendConfig.respondType = responseTypeOK
				},
			},
		},
	}

	for _, test := range tests {
		ts, clean := newTestSuite(t, tc)
		ts.executeMultiCmd(t, test.cfgs)
		clean()
	}
}

func TestBeginStmt(t *testing.T) {
	tests := []struct {
		stmt    string
		isBegin bool
	}{
		{
			stmt:    "begin",
			isBegin: true,
		},
		{
			stmt:    "BEGIN",
			isBegin: true,
		},
		{
			stmt:    "begin optimistic as of timestamp now()",
			isBegin: true,
		},
		{
			stmt:    "    begin",
			isBegin: true,
		},
		{
			stmt:    "start transaction",
			isBegin: true,
		},
		{
			stmt:    "START transaction",
			isBegin: true,
		},
		{
			stmt:    "start transaction with consistent snapshot",
			isBegin: true,
		},
		{
			stmt:    "begin; select 1",
			isBegin: true,
		},
		{
			stmt:    "/*+ some_hint */begin",
			isBegin: true,
		},
		{
			stmt:    "commit",
			isBegin: false,
		},
		{
			stmt:    "select 1; begin",
			isBegin: false,
		},
	}
	for _, test := range tests {
		require.Equal(t, test.isBegin, isBeginStmt(test.stmt))
	}
}

// Test forwarding multi-statements works well.
func TestMultiStmt(t *testing.T) {
	tc := newTCPConnSuite(t)

	// COM_STMT_PREPARE don't support multiple statements, so we only test COM_QUERY.
	cfgs := []cfgOverrider{
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = pnet.ComQuery
			cfg.backendConfig.respondType = responseTypeOK
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = pnet.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = pnet.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.rows = 1
			cfg.backendConfig.stmtNum = 2
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = pnet.ComQuery
			cfg.backendConfig.respondType = responseTypeResultSet
			cfg.backendConfig.columns = 1
			cfg.backendConfig.stmtNum = 3
		},
		func(cfg *testConfig) {
			cfg.clientConfig.cmd = pnet.ComQuery
			cfg.backendConfig.respondType = responseTypeLoadFile
			cfg.backendConfig.stmtNum = 2
		},
	}

	for _, cfg := range cfgs {
		ts, clean := newTestSuite(t, tc, cfg)
		ts.executeCmd(t, nil)
		clean()
	}
}

// Test that the proxy won't hang or panic when a network error happens.
func TestNetworkError(t *testing.T) {
	tc := newTCPConnSuite(t)

	clientExitCfg := func(cfg *testConfig) {
		cfg.clientConfig.abnormalExit = true
	}
	backendExitCfg := func(cfg *testConfig) {
		cfg.backendConfig.abnormalExit = true
	}
	clientErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
		require.True(t, pnet.IsDisconnectError(ts.mc.err))
	}
	backendErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
		require.True(t, pnet.IsDisconnectError(ts.mb.err))
	}
	proxyErrChecker := func(t *testing.T, ts *testSuite) {
		require.True(t, pnet.IsDisconnectError(ts.mp.err))
	}

	ts, clean := newTestSuite(t, tc, clientExitCfg)
	ts.authenticateFirstTime(t, backendErrChecker)
	require.Equal(t, SrcClientNetwork, Error2Source(ts.mp.err))
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.authenticateFirstTime(t, clientErrChecker)
	require.Equal(t, ErrBackendHandshake, ErrToClient(ts.mp.err))
	require.Equal(t, SrcBackendNetwork, Error2Source(ts.mp.err))
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.authenticateSecondTime(t, proxyErrChecker)
	clean()

	ts, clean = newTestSuite(t, tc, clientExitCfg)
	ts.executeCmd(t, backendErrChecker)
	require.Equal(t, SrcClientNetwork, Error2Source(ts.mp.err))
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.executeCmd(t, clientErrChecker)
	require.Equal(t, SrcBackendNetwork, Error2Source(ts.mp.err))
	clean()

	ts, clean = newTestSuite(t, tc, backendExitCfg)
	ts.query(t, proxyErrChecker)
	clean()
}

// Test that TiProxy won't panic when query encounters an error.
func TestQueryError(t *testing.T) {
	tc := newTCPConnSuite(t)
	tests := []struct {
		cfg cfgOverrider
		c   checker
	}{
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.abnormalExit = true
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeErr
			},
		},
		{
			cfg: func(cfg *testConfig) {
				cfg.backendConfig.respondType = responseTypeResultSet
				cfg.backendConfig.columns = 1
				cfg.backendConfig.exitInResult = true
			},
		},
	}
	for _, test := range tests {
		ts, clean := newTestSuite(t, tc, test.cfg)
		ts.query(t, func(t *testing.T, ts *testSuite) {})
		clean()
	}
}
