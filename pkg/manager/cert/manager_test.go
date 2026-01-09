// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cert

import (
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mengchengtech/cerberus/lib/config"
	"github.com/mengchengtech/cerberus/lib/util/logger"
	"github.com/mengchengtech/cerberus/lib/util/security"
	"github.com/mengchengtech/cerberus/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
)

func connectWithTLS(ctls, stls *tls.Config) (clientErr, serverErr error) {
	client, server := net.Pipe()
	var wg waitgroup.WaitGroup
	wg.Run(func() {
		tlsConn := tls.Client(client, ctls)
		clientErr = tlsConn.Handshake()
		_ = client.Close()
	})
	wg.Run(func() {
		tlsConn := tls.Server(server, stls)
		serverErr = tlsConn.Handshake()
		_ = server.Close()
	})
	wg.Wait()
	return
}

// Test various configurations.
func TestInit(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	tmpdir := t.TempDir()

	type testcase struct {
		name  string
		cfg   config.Config
		check func(*testing.T, *CertManager)
		err   string
	}
	// should not care much about the internal of *tls.Config,
	// which should be well-tested in pkg/lib/util/security/.
	cases := []testcase{
		{
			name: "empty",
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.ServerSQLTLS())
				require.Nil(t, cm.SQLTLS())
			},
		},
		{
			name: "server config",
			cfg: config.Config{
				Security: config.Security{
					ServerSQLTLS: config.TLSConfig{AutoCerts: true},
					SQLTLS:       config.TLSConfig{AutoCerts: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.Nil(t, cm.SQLTLS())
				require.NotNil(t, cm.ServerSQLTLS())
			},
		},
		{
			name: "client config",
			cfg: config.Config{
				Security: config.Security{
					ServerSQLTLS: config.TLSConfig{SkipCA: true},
					SQLTLS:       config.TLSConfig{SkipCA: true},
				},
			},
			check: func(t *testing.T, cm *CertManager) {
				require.NotNil(t, cm.SQLTLS())
				require.Nil(t, cm.ServerSQLTLS())
			},
		},
		{
			name: "invalid config",
			cfg: config.Config{
				Security: config.Security{
					SQLTLS: config.TLSConfig{CA: filepath.Join(tmpdir, "ca")},
				},
			},
			err: "no such file or directory",
		},
	}

	for i, tc := range cases {
		t.Logf("testcase[%d] start: %+v\n", i, tc)

		certMgr := NewCertManager()
		certMgr.SetRetryInterval(100 * time.Millisecond)
		err := certMgr.Init(&tc.cfg, lg)
		if tc.err != "" {
			require.ErrorContains(t, err, tc.err, fmt.Sprintf("%+v", tc))
		} else {
			require.NoError(t, err)
		}
		if tc.check != nil {
			tc.check(t, certMgr)
		}
	}
}

func TestBidirectional(t *testing.T) {
	tmpdir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	caPath1 := filepath.Join(tmpdir, "c1", "ca")
	keyPath1 := filepath.Join(tmpdir, "c1", "key")
	certPath1 := filepath.Join(tmpdir, "c1", "cert")
	caPath2 := filepath.Join(tmpdir, "c2", "ca")
	keyPath2 := filepath.Join(tmpdir, "c2", "key")
	certPath2 := filepath.Join(tmpdir, "c2", "cert")

	require.NoError(t, security.CreateTLSCertificates(lg, certPath1, keyPath1, caPath1, 0, security.DefaultCertExpiration))
	require.NoError(t, security.CreateTLSCertificates(lg, certPath2, keyPath2, caPath2, 0, security.DefaultCertExpiration))

	cfg := &config.Config{
		Workdir: tmpdir,
		Security: config.Security{
			ServerSQLTLS: config.TLSConfig{
				Cert: certPath1,
				Key:  keyPath1,
				CA:   caPath2,
			},
			SQLTLS: config.TLSConfig{
				CA:   caPath1,
				Key:  keyPath2,
				Cert: certPath2,
			},
		},
	}

	certMgr := NewCertManager()
	require.NoError(t, certMgr.Init(cfg, lg))
	stls := certMgr.ServerSQLTLS()
	ctls := certMgr.SQLTLS()
	clientErr, serverErr := connectWithTLS(ctls, stls)
	require.NoError(t, clientErr)
	require.NoError(t, serverErr)
}
