// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mengchengtech/cerberus/pkg/sctx"
	"github.com/mengchengtech/cerberus/pkg/server"
	"github.com/mengchengtech/cerberus/pkg/util/errors"
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

	rootCmd.PersistentFlags().StringVar(&sctx.ConfigFile, "config", "", "proxy config file path")

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

	RunRootCommand(rootCmd)
}

func RunRootCommand(rootCmd *cobra.Command) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sc := make(chan os.Signal, 1)
		signal.Notify(sc,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT,
		)

		// wait for quit signals
		<-sc
		cancel()
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}
