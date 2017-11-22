package main

import (
	"sync"

	"github.com/gstormlee/gstorm/core/etcd"
	"github.com/gstormlee/gstorm/core/topology"
)

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
