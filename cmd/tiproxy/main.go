// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/mengchengtech/cerberus/lib/util/cmd"
	"github.com/mengchengtech/cerberus/lib/util/errors"
	"github.com/mengchengtech/cerberus/pkg/sctx"
	"github.com/mengchengtech/cerberus/pkg/server"
	"github.com/mengchengtech/cerberus/pkg/util/versioninfo"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the proxy server",
		Version: fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash),
	}
	rootCmd.SetOutput(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	sctx := &sctx.Context{}

	var deprecatedStr, configInfo string
	rootCmd.PersistentFlags().StringVar(&sctx.ConfigFile, "config", "", "proxy config file path")
	rootCmd.PersistentFlags().StringVar(&deprecatedStr, "log_encoder", "", "deprecated and will be removed")
	rootCmd.PersistentFlags().StringVar(&deprecatedStr, "log_level", "", "deprecated and will be removed")
	rootCmd.PersistentFlags().StringVar(&sctx.AdvertiseAddr, "advertise-addr", "", "advertise address")
	rootCmd.PersistentFlags().StringVar(&configInfo, "config-info", "", "output config info and exit")

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		srv, err := server.NewServer(cmd.Context(), sctx)
		if err != nil {
			return errors.Wrapf(err, "fail to create server")
		}

		<-cmd.Context().Done()
		if e := srv.Close(); e != nil {
			err = errors.Wrapf(err, "shutdown with errors")
		}

		return err
	}

	cmd.RunRootCommand(rootCmd)
}
