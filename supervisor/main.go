package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// RootCmd var
var RootCmd = &cobra.Command{
	Use:   "supervisor",
	Short: "supervisor config application",
	Long:  "supervisor config application client",
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.AddCommand(
		NewConfigCommand(),
	)
}
