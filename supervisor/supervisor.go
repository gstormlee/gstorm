package main

import (
	"fmt"
	"os/user"
	"path"
	"sync"
	"time"

	"github.com/gstormlee/gstorm/core/etcd"
)

// Supervisor struct
type Supervisor struct {
	EtcdAddr   string
	Name       string
	EtcdClient *etcd.Client
	Topologies map[string]*Topology
	SavePath   string
}

// Super Supervisor
var Super *Supervisor
var once sync.Once
var etcdAddr string
var name string

// GetInstance func
func GetInstance() *Supervisor {
	once.Do(func() {
		Super = &Supervisor{}
		Super.EtcdAddr = etcdAddr
		Super.EtcdClient = etcd.NewClient(etcdAddr)
		Super.Name = name
		Super.Topologies = make(map[string]*Topology)
		user, err := user.Current()
		if err != nil {
			panic(err)
		}
		Super.SavePath = path.Join(user.HomeDir, "supervisor")
	})
	return Super
}

// Register func
func Register() {
	data := GetInstance()
	key := "/nimbus/clients/" + data.Name
	data.EtcdClient.Grant(key, "100", 3)
	go Refresh()
}

// Refresh func
func Refresh() {
	timer := time.NewTicker(time.Second)
	for {
		select {
		case <-timer.C:
			data := GetInstance()
			key := "/nimbus/clients/" + data.Name
			data.EtcdClient.KeepAliveOnce(key, "100", 3)
		}
	}
}
