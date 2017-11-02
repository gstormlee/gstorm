package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// RootCmd var
var RootCmd = &cobra.Command{
	Use:   "storm",
	Short: "strom distribute application",
	Long:  "strom distribute application client",
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },

}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.AddCommand(
		NewDistributeCommand(),
	)
}