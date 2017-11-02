package main

import (
	"errors"

	"github.com/gstormlee/gstorm/transfer"

	"github.com/gstormlee/gstorm/nimbus/distribute"

	"io"
	"io/ioutil"

	"github.com/spf13/cobra"
)

//var EtcdAddr string

// NewConfigCommand func
func NewConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config etcd",
		Short: "config  the given etcd address",
		Long:  "",
		Run:   configCommand,
	}
	cmd.Flags().StringVar(&distribute.EtcdAddr, "etcd", "e", "eted ip and port")
	return cmd
}

// configCommand func
func configCommand(cmd *cobra.Command, args []string) {

	//go
	startServer()
}

func argOrStdin(args []string, stdin io.Reader, i int) (string, error) {
	if i < len(args) {
		return args[i], nil
	}
	bytes, err := ioutil.ReadAll(stdin)
	if string(bytes) == "" || err != nil {
		return "", errors.New("no available argument and stdin")
	}
	return string(bytes), nil
}

func startServer() {
	data := distribute.GetInstance()
	key := "/nimbus/clients"
	data.EtcdClient.DeleteWithPreFix(key)

	go distribute.WatchSupervisor()

	server := transfer.NewServer()
	server.ListenAndServe(data.EtcdAddr)
}
