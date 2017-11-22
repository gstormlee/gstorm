package utils 

// import (
// 	"src/core/etcd"
// 	"sync"
// )

// type Storm struct {
// 	EtcdAddr   string
// 	EtcdClient *etcd.Client
// }

// var (
// 	storm *Storm
// 	once  sync.Once
// 	//EtcdAddr string
// )

// func GetSingletonStorm() *Storm {
// 	once.Do(func() {
// 		storm = &Storm{EtcdAddr: "127.0.0.1"}
// 		storm.EtcdClient = etcd.NewClient(storm.EtcdAddr)

// 	})
// 	return storm
// }
