package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/user"
	"reflect"
	"strconv"

	"github.com/gstormlee/gstorm/core/etcd"
	"github.com/gstormlee/gstorm/core/send"
	"github.com/gstormlee/gstorm/core/tuple"
	"github.com/gstormlee/gstorm/core/utils"

	"github.com/gstormlee/gstorm/core/topology/group"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// ReadFromEtcd func
func (t *Topology) ReadFromEtcd() {
	key := "/topology/" + t.TopologyName
	key1 := key + "/spouts"
	t.ReadNodes(key1)
	key1 = key + "/bolts"
	t.ReadNodes(key1)
	key1 = key + "/ackers"
	t.ReadNodes(key1)
}

// ReadNodes func
func (t *Topology) ReadNodes(key string) error {
	vals, err := t.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, v := range vals {
		node := &Node{}
		if err = json.Unmarshal(utils.Slice(v), node); err == nil {
			t.AllNodes[node.Name] = node
		} else {
			fmt.Println("read error", err)
		}
	}
	return nil
}

// CreateSerial func
func (t *Topology) CreateSerial() (int, error) {
	s1, err := concurrency.NewSession(t.EtcdClient.Etcd)
	defer s1.Close()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	m1 := concurrency.NewMutex(s1, "/my-lock/")
	if err1 := m1.Lock(context.TODO()); err1 != nil {
		fmt.Println(err1)
		return 0, err1
	}
	defer m1.Unlock(context.TODO())
	vals, err2 := t.EtcdClient.Get("/serial/" + t.TopologyName)
	if err2 != nil {
		fmt.Println(err2)
		return 0, err2
	}
	if len(vals) == 1 {
		serial, err3 := strconv.Atoi(vals[0])
		if err3 != nil {
			return 0, err3
		}
		serial++
		str := strconv.Itoa(serial)
		t.EtcdClient.Set("/serial/ "+t.TopologyName, str)
		return serial, nil
	} else if len(vals) == 0 {
		t.EtcdClient.Set("/serial/ "+t.TopologyName, "1")
		return 1, nil
	}
	return 0, errors.New("can not found key")
}

// Topology struct
type Topology struct {
	Name           string
	Serial         int
	Watch          WatchWorker
	Ends           []*Node
	Ackers         []*Node
	Starts         []ISpout
	StartNodes     []*Node
	CreatedWorkers []IHandle
	acker          IHandle
	AckerGrouping  group.IGrouping
	ServerName     string
	TopologyName   string
	EtcdClient     *etcd.Client
	Factories      Factories
	AllNodes       map[string]*Node
	AckerWait      chan int
	WatchNodeChan  chan string
	MessageFactory send.IMessageFactory
}

// NewTopology func
func NewTopology(serverName, topologyName string, etcdClient *etcd.Client) *Topology {
	t := &Topology{}
	t.TopologyName = topologyName
	t.ServerName = serverName
	t.EtcdClient = etcdClient
	t.AllNodes = make(map[string]*Node)
	t.AckerWait = make(chan int)
	t.WatchNodeChan = make(chan string, 10)
	return t
}

// Start func
func (t *Topology) Start(name string) {
	var s *Topology
	fmt.Println("storm", s)

	t.AckerWait = make(chan int)
	t.ReadFromEtcd()
	t.Watch = NewWatchWorker()
	fmt.Println("read finish")

	factory := &Factory{}
	t.Register(factory)
	serial, err := t.CreateSerial()
	if err != nil {
		fmt.Println("create serial error:", err)
		return
	}
	gen := GetGlobalGenerator()
	gen.SetSerial(serial)
	fmt.Println("serial", serial)

	key := "/distribute/" + t.TopologyName + "/" + t.ServerName

	vals, err := t.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		fmt.Println("error ", err)
		return
	}
	for _, v := range vals {
		node := &Node{}

		err = json.Unmarshal(utils.Slice(v), node)
		if err != nil {
			fmt.Println("json parser error", err)
			return
		}

		if node.NType == "spout" {
			t.StartNodes = append(t.StartNodes, node)
		}

		if node.NType == "acker" {
			t.Ackers = append(t.Ackers, node)
		} else if t.NextNodeIsEnd(node) {
			t.Ends = append(t.Ends, node)
		} else {
			var arr []string
			arr, _ = t.Watch.Node2Name[node]

			if next := node.GetNext(); next != nil {
				for _, k := range next {
					if v, ok := t.Watch.Name2Node[k]; ok {
						v = append(v, node)
						t.Watch.Name2Node[k] = v
					} else {
						var a []*Node
						a = append(a, node)
						t.Watch.Name2Node[k] = a
					}

					arr = append(arr, k)
				}
			}

			t.Watch.Node2Name[node] = arr
		}
	}
	// for k, v := range t.Watch.Node2Name {
	// 	fmt.Println("node 2 name ********************", k.Name, k.NodeName, v)
	// }

	// for k, v := range t.Watch.Name2Node {
	// 	fmt.Println("name 2 node*****************************", k)
	// 	for _, n := range v {
	// 		fmt.Println(n.Name, n.NodeName)
	// 	}
	// }
	if len(t.Ackers) > 0 {
		go t.WatchAcker(t.Ackers[0])
		for _, v := range t.Ackers {
			fmt.Printf("create node acker = %v\n", v)
			t.CreateNode(v)
		}
		<-t.AckerWait
	}
	go t.WatchAll()
	t.CreateEndNode()
}

// NextNodeIsEnd func
func (t *Topology) NextNodeIsEnd(node *Node) bool {
	if node.MasterGrouping == nil {
		return true
	}
	next := node.GetNext()
	for _, v := range next {
		if v != "end" {
			return false
		}
	}
	return true
}

// WatchAll func
func (t *Topology) WatchAll() {
	for k, _ := range t.Watch.Name2Node {
		t.WatchNext(t.TopologyName, k)
	}
	for {
		name := <-t.WatchNodeChan
		t.CreateNextChan(name)
		t.Watch.Ready = append(t.Watch.Ready, name)

		if v, ok := t.Watch.Name2Node[name]; ok {
			for _, node := range v {
				if t.Watch.IsReady(t.Watch.Node2Name[node]) {
					t.CreateNode(node)
				}
			}
		}
	}
}

// CreateNextChan func
func (t *Topology) CreateNextChan(next string) {
	key := "/real/" + t.TopologyName + "/" + next + "/"
	values, err := t.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		fmt.Println(err)
		return
	}
	var chans []chan tuple.IID
	for _, addr := range values {
		var sender send.ISender
		if next == "AckerBolt" {
			sender = NewAckerSender(addr)
		} else {
			sender = send.NewRpcSender(addr)
		}

		sender.Prepare()
		chans = append(chans, sender.GetOutchan())
		go sender.Run()
	}
	t.Watch.NextChans[next] = chans
}

// CreateEndNode func
func (t *Topology) CreateEndNode() {
	for _, worker := range t.Ends {
		t.CreateNode(worker)
	}
}

// GetParallelNumber func
func (t *Topology) GetParallelNumber(name string, node string) (int, error) {

	if n, ok := t.AllNodes[node]; ok {
		return n.Num, nil
	}
	return 0, errors.New("no node")
}

// WatchAcker func
func (t *Topology) WatchAcker(node *Node) {
	num := node.Num
	go t.WatchKey(node.Name, num)
	name := <-t.WatchNodeChan
	t.CreateNextChan(name)
	t.CreateAckerGrouping(name)
}

// CreateAckerGrouping func
func (t *Topology) CreateAckerGrouping(next string) {
	gd := GroupingData{}
	gd.Field = "ID"
	gd.GType = "FieldGrouping"
	gd.Next = next
	if g, err := t.CreateGrouping(gd); err == nil {
		t.AckerGrouping = g
		t.AckerWait <- 1
	}

}

// WatchNext func
func (t *Topology) WatchNext(topology, next string) {
	num, err := t.GetParallelNumber(topology, next)
	if err != nil {
		fmt.Println(err)
		return
	}

	go t.WatchKey(next, num)
}

// WatchKey func
func (t *Topology) WatchKey(next string, num int) {
	key := "/real/" + t.TopologyName + "/" + next + "/"
	// if t.ReadWatchKey(next, num) {
	// 	return
	// }
	rch := t.EtcdClient.Etcd.Watch(context.Background(), key, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				t.ReadWatchKey(next, num)
				break
				//return
			case mvccpb.DELETE:

			}
		}
	}

}

// ReadWatchKey func
func (t *Topology) ReadWatchKey(next string, num int) bool {
	key := "/real/" + t.TopologyName + "/" + next + "/"
	values, err := t.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		return false
	}
	if len(values) == num {
		t.WatchNodeChan <- next
		return true
	}
	return false
}

// CreateNode  func
func (t *Topology) CreateNode(node *Node) {
	worker := t.Factories.CreateNodeFactory(node.Name, t.ServerName, node.NodeName)
	if worker == nil {
		fmt.Printf("fatal error:create node %s is nil\n", node.Name)
	}

	t.CreatedWorkers = append(t.CreatedWorkers, worker)

	if s, ok := worker.(ISpout); ok {
		r := t.CreateAckerResultReciever(node, worker)
		r.SetQueue(s.GetQueue())
		s.SetAckerReciever(r)
	} else {
		t.CreateReciever(node, worker)
	}
	if node.MasterGrouping != nil {
		mg := t.CreateMasterGrouping(node)
		if mg != nil {
			mg.Lanuch()
			worker.SetMasterGrouping(mg)
		}
	}
	bolt, ok1 := worker.(IBolt)
	if ok1 {
		bolt.Prepare()
		go bolt.Run()
		return
	}
	value, ok := worker.(ISpout)
	if ok {
		t.Starts = append(t.Starts, value)
		if len(t.Starts) == len(t.StartNodes) {
			t.StartCommand()
		}
	}
}

// CreateMasterGrouping func
func (t *Topology) CreateMasterGrouping(node *Node) group.IMasterGrouping {
	mg := t.Factories.CreateMasterGrouping(node.MasterGrouping)
	for _, n := range node.MasterGrouping.Groupings {
		g, err := t.CreateGrouping(n)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		if g != nil {
			mg.AddGrouping(n.Next, g)
		} else {
			return nil
		}
	}
	return mg
}

// CreateGrouping func
func (t *Topology) CreateGrouping(grouping GroupingData) (group.IGrouping, error) {
	g := t.Factories.CreateGrouping(grouping)
	chans := t.Watch.NextChans[grouping.Next]
	if g != nil {
		g.Prepare(chans)
		g.Launch()
		return g, nil
	}
	return nil, errors.New("create Grouping " + grouping.Next + "not found constructor")
}

// CreateReciever func
func (t *Topology) CreateReciever(worker *Node, node IHandle) {
	key := "/real/" + t.TopologyName + "/" + worker.Name + "/" + worker.NodeName
	addr, err2 := t.GetAddr(t.ServerName)
	if err2 != nil {
		fmt.Println(err2)
		return
	}
	var r send.IReciever
	t.EtcdClient.Set(key, addr)
	if worker.NType == "acker" {
		r = NewAckerReciever(addr)
	} else {
		r = send.NewReciever(addr)
	}

	go r.ListenAndServe(node.GetInchan(), t.MessageFactory)
}

// CreateAckerResultReciever func
func (t *Topology) CreateAckerResultReciever(worker *Node, node IHandle) *AckerResultReciever {
	addr, err2 := t.GetAddr(t.Name)
	if err2 != nil {
		fmt.Println(err2)
		return nil
	}
	//gstorm.EtcdClient.Set(key, addr)
	r := NewAckerResultReciever(addr)
	go r.ListenAndServe(node.GetInchan())
	return r
}

// StartCommand func
func (t *Topology) StartCommand() {
	if t.Ackers != nil {
		for _, node := range t.CreatedWorkers {
			w := reflect.TypeOf(node)
			if w.String() != "*topology.AckerBolt" {
				node.SetAckerGrouping(t.AckerGrouping)
			}
		}
	}

	for _, node := range t.Starts {
		files := make(map[string]string)
		str, _ := user.Current()
		files["1"] = str.HomeDir + "/my.txt"
		node.Open(files)
		go node.Run()
		node.Launch()
	}
}

// GetAddr name mechine name
func (t *Topology) GetAddr(server string) (string, error) {
	ip, err := utils.GetLocalIP()
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	p := utils.GetSingletonPort()
	port, err1 := p.GetLocalPort(t.ServerName, t.EtcdClient)
	if err1 != nil {
		fmt.Println(err1)
		return "", err1
	}
	addr := ip + ":" + strconv.Itoa(port)
	return addr, nil
}
