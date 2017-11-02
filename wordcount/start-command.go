package main

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/topology"

	"github.com/spf13/cobra"
)

var (
	name         string
	topologyName string
	ackerWait    chan int
)

func NewStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start etcd",
		Short: "start the given etcd address",
		Long:  "",
		Run:   startCommand,
	}

	cmd.Flags().StringVar(&EtcdAddr, "etcd", "e", "eted ip and port")
	cmd.Flags().StringVar(&name, "name", "n", "server name")
	cmd.Flags().StringVar(&topologyName, "topology", "t", "topology name")

	return cmd
}

// configCommand func
func startCommand(cmd *cobra.Command, args []string) {
	storm := GetStorm()
	//gstorm := topoelogy.GetGstorm()
	//gstorm.Init(storm.Name, storm.TopologyName, storm.EtcdClient)

	t := topology.NewTopology(storm.Name, storm.TopologyName, storm.EtcdClient)

	storm.Builders[storm.TopologyName] = t
	f := &Factory{}
	t.Register(f)
	topology.Distribute(storm.TopologyName, t)
	ch := make(chan int)
	a := <-ch
	fmt.Println(a)
	//var _ topology.ISpout = new(SentenceSpout)
}
