// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/mengchengtech/cerberus/lib/cli"
	"github.com/mengchengtech/cerberus/lib/util/cmd"
	"github.com/mengchengtech/cerberus/pkg/util/versioninfo"
)

func main() {
	rootCmd := cli.GetRootCmd(nil)
	rootCmd.Version = fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash)
	rootCmd.Use = strings.Replace(rootCmd.Use, "tiproxyctl", os.Args[0], 1)
	cmd.RunRootCommand(rootCmd)
}
