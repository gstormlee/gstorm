package distribute

import (
	"context"

	"strings"
	"sync"

	"github.com/gstormlee/gstorm/core/etcd"
	"github.com/gstormlee/gstorm/core/utils"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// Nimbus struct
type Nimbus struct {
	EtcdAddr    string
	EtcdClient  *etcd.Client
	Supervisors *utils.Set
	Datas       map[string]StormData
}

var (
	Instance   *Nimbus
	once       sync.Once
	EtcdAddr   string
	WaitChanel chan string
)

// GetInstance func
func GetInstance() *Nimbus {
	once.Do(func() {
		Instance = &Nimbus{EtcdAddr: EtcdAddr}
		Instance.EtcdClient = etcd.NewClient(Instance.EtcdAddr)
		Instance.Supervisors = utils.NewSet()
		Instance.Datas = make(map[string]StormData)
	})
	return Instance
}

// WatchSupervisor func
func WatchSupervisor() {
	key := "/nimbus/clients"
	data := GetInstance()
	rch := data.EtcdClient.Etcd.Watch(context.Background(), key, clientv3.WithPrefix())
	//var names []string
	for wresp := range rch {
		for _, ev := range wresp.Events {
			a := strings.Split(string(ev.Kv.Key[:]), "/")
			key := a[len(a)-1]
			switch ev.Type {
			case mvccpb.PUT:
				data.Supervisors.Add(key) //string(ev.Kv.Key[:]))
			case mvccpb.DELETE:
				data.Supervisors.Remove(key) //string(ev.Kv.Key[:]))
			}
		}
	}
}
