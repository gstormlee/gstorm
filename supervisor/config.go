package main

import "github.com/spf13/cobra"

// NewConfigCommand func
func NewConfigCommand() *cobra.Command {
	//data := GetInstance()
	cmd := &cobra.Command{
		Use:   "config [options] <key> [range_end]",
		Short: "config  the given etcd address ans client name",
		Long:  "",
		Run:   configCommand,
	}
	cmd.Flags().StringVar(&etcdAddr, "etcd", "e", "eted ip and port")
	cmd.Flags().StringVar(&name, "name", "n", "supervisor name")
	return cmd
}
func configCommand(cmd *cobra.Command, args []string) {
	//data := GetInstance()

	Register()
	t := NewTopology()
	t.Watch()
}
