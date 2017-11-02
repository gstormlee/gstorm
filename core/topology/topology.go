package topology

import (
	"github.com/gstormlee/gstorm/core/topology/group"

	"github.com/gstormlee/gstorm/core/etcd"
)

// Worker struct
type Worker struct {
	Name     string
	NodeName string
	Index    int
	Next     string
	Start    bool
	Grouping string
	NType    string
	Field    string
}

// Topology struct
type Topology struct {
	Name           string
	Serial         int
	WatchNodes     map[string][]*Worker
	Ends           []*Worker
	Ackers         []*Worker
	Starts         []ISpout
	StartWorkers   []*Worker
	CreatedWorkers []IHandle
	acker          IHandle
	AckerGrouping  group.IGrouping
	ServerName     string
	TopologyName   string
	EtcdClient     *etcd.Client
	Factorys       []IFactory
	init           bool
}

// NewTopology func
func NewTopology(serverName, topologyName string, etcdClient *etcd.Client) *Topology {

	t := &Topology{}
	t.WatchNodes = make(map[string][]*Worker)
	t.TopologyName = topologyName
	t.ServerName = serverName
	t.EtcdClient = etcdClient

	return t
}

// Register func
func (t *Topology) Register(factory IFactory) {
	t.Factorys = append(t.Factorys, factory)
}
