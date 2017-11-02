package main

import (
	"sync"

	"github.com/gstormlee/gstorm/core/topology"

	"github.com/gstormlee/gstorm/core/etcd"
)

// // Worker struct
// type Worker struct {
// 	Name     string
// 	NodeName string
// 	Index    int
// 	Next     string
// 	Start    bool
// 	Grouping string
// 	NType    string
// 	Field    string
// }

// Storm struct
type Storm struct {
	Serial       int
	Name         string
	EtcdAddr     string
	TopologyName string
	EtcdClient   *etcd.Client
	Builders     map[string]*topology.Topology
}

var storm *Storm
var once sync.Once
var EtcdAddr string

// GetStorm func
func GetStorm() *Storm {

	once.Do(func() {
		storm = &Storm{}
		storm.Name = name
		storm.TopologyName = topologyName
		storm.EtcdClient = etcd.NewClient(EtcdAddr)
		storm.Builders = make(map[string]*topology.Topology)
		//storm.WatchNodes = make(map[string][]*Worker)
	})
	return storm
}
