package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/user"
	"reflect"
	"strconv"

	"github.com/gstormlee/gstorm/core/send"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/gstormlee/gstorm/core/topology/group"

	"github.com/gstormlee/gstorm/core/data"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	name         string
	topologyName string
	gAckerWait   chan int
)

// NodeData struct
type NodeData struct {
	Name     string
	Num      int
	Next     string
	Grouping string
	index    int
}

// TopologyData struct
type TopologyData struct {
	Name   string
	Begin  string
	Spouts []*NodeData
	Bolts  map[string]*NodeData
}

// CreateSerial func
func CreateSerial() (int, error) {
	gstorm := GetGstorm()
	s1, err := concurrency.NewSession(gstorm.EtcdClient.Etcd)
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
	vals, err2 := gstorm.EtcdClient.Get("/serial/" + gstorm.TopologyName)
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
		gstorm.EtcdClient.Set("/serial/ "+gstorm.TopologyName, str)
		return serial, nil
	} else if len(vals) == 0 {
		gstorm.EtcdClient.Set("/serial/ "+gstorm.TopologyName, "1")
		return 1, nil
	}
	return 0, errors.New("can not found key")
}

var storm *Topology

// GetGstorm func
func GetGstorm() *Topology {
	return storm
}

// Distribute func
func Distribute(name string, gstorm *Topology) {
	storm = gstorm
	factory := &Factory{}
	gstorm.Register(factory)
	serial, err := CreateSerial()
	if err != nil {
		fmt.Println("create serial error:", err)
		return
	}
	gstorm.Serial = serial

	key := "/distribute/" + gstorm.TopologyName + "/" + gstorm.ServerName
	fmt.Println("distribute", key)
	vals, err := gstorm.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		fmt.Println("error ", err)
		return
	}
	for _, v := range vals {
		worker := &Worker{}

		err = json.Unmarshal(data.Slice(v), worker)
		fmt.Println("one worker", worker)
		if err != nil {
			fmt.Println(err)
			return
		}
		if worker.Start {
			gstorm.StartWorkers = append(gstorm.StartWorkers, worker)
		}
		if worker.Next == "end" && worker.NType == "acker" {
			gstorm.Ackers = append(gstorm.Ackers, worker)
		} else if worker.Next == "end" {
			gstorm.Ends = append(gstorm.Ends, worker)
		} else {
			if v, ok := gstorm.WatchNodes[worker.NodeName]; ok {
				v = append(v, worker)
				gstorm.WatchNodes[worker.NodeName] = v
			} else {
				var a []*Worker
				a = append(a, worker)
				gstorm.WatchNodes[worker.NodeName] = a
			}
		}
	}

	fmt.Println("ackers number:", len(gstorm.Ackers))

	if len(gstorm.Ackers) > 0 {
		gAckerWait = make(chan int)
		go WatchAcker(gstorm.TopologyName, gstorm.Ackers[0])
		var chans []chan tuple.IID
		for _, v := range gstorm.Ackers {
			CreateOneNode(v, chans)
		}
		a := <-gAckerWait
		fmt.Println(a)
	}
	Watch()
	CreateEndNode()
}

// Watch func
func Watch() {
	gstorm := GetGstorm()
	fmt.Println("watch nodes----+++++!!!!!!", gstorm.WatchNodes)
	for _, v := range gstorm.WatchNodes {
		go WatchNodeReal(gstorm.TopologyName, v[0])
	}
}

// CreateEndNode func
func CreateEndNode() {
	gstorm := GetGstorm()
	for _, worker := range gstorm.Ends {
		var chans []chan tuple.IID
		CreateOneNode(worker, chans)
	}
}

// GetParallelNumber func
func GetParallelNumber(name string, node string) (int, error) {
	gstorm := GetGstorm()
	key := "/topology/" + name + "/" + node + "/num"
	fmt.Println("GetParallelNumber", key, name)
	vals, err := gstorm.EtcdClient.Get(key)
	if err != nil {
		fmt.Println("error:", err)
		return 0, err
	}
	if len(vals) == 1 {
		num, err1 := strconv.Atoi(vals[0])
		if err1 != nil {
			fmt.Println(err1)
			return 0, err1
		}
		return num, nil
	}
	return 0, errors.New("muliti return value")
}

// CreateWorker func
func CreateWorker(key string, num int, worker *Worker) (bool, error) {
	fmt.Println("create worker", worker)
	gstorm := GetGstorm()
	values, err := gstorm.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		return false, err
	}
	if len(values) == num {
		var a []string
		for _, v := range values {
			a = append(a, v)
		}

		if len(a) == num {
			fmt.Println("crete nodes", worker, a)
			CreateNodes(worker, a)
			return false, nil
		}
	}

	return false, nil
}

// WatchAcker func
func WatchAcker(name string, worker *Worker) {
	gstorm := GetGstorm()
	num, err := GetParallelNumber(name, worker.NodeName)
	if err != nil {
		fmt.Println("GetParallelNumber", err)
		return
	}
	key := "/real/" + name + "/" + worker.NodeName + "/"
	fmt.Println("watch acker key:", key, name, worker.NodeName)
	g, err1 := CreateAckerGrouping(key, worker, num)
	gstorm.AckerGrouping = g
	if err1 != nil {
		fmt.Println(err)
		//return
	}

	rch := gstorm.EtcdClient.Etcd.Watch(context.Background(), key, clientv3.WithPrefix())

	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			switch ev.Type {
			case mvccpb.PUT:
				//key = "/real/" + name + "/" + node + "/"
				group, err2 := CreateAckerGrouping(key, worker, num)
				if err2 != nil {
					fmt.Println(err2)
				} else {
					gstorm.AckerGrouping = group
				}
				break
			case mvccpb.DELETE:
				//t.Name.Remove(key1)
				//delete(t.groupings, key1)
			}
		}
	}

}

// CreateAckerGrouping func
func CreateAckerGrouping(key string, worker *Worker, num int) (group.IGrouping, error) {
	gstorm := GetGstorm()
	//key := "/real/" + name + "/" + worker.NodeName + "/"
	fmt.Println("CreateAckerGrouping", key)
	values, err := gstorm.EtcdClient.GetSortedPrefix(key)
	if err != nil {
		return nil, nil
	}

	if len(values) == num {
		var a []string
		for _, v := range values {
			a = append(a, v)
		}

		if len(a) == num {
			var chans []chan tuple.IID
			fmt.Println("+++++++++++++++++create Acker Field Grouping--------------------", values)
			for _, v := range values {
				var sender send.ISender
				sender = NewAckerSender(v)
				sender.Prepare()
				chans = append(chans, sender.GetOutchan())
				go sender.Run()
			}
			g := CreateGrouping("FieldGrouping", "ID")
			fmt.Println("create grouping0000000000iî11111111111111111", reflect.TypeOf(g))
			if g != nil {
				g.Prepare(chans)
				g.Launch()
				gAckerWait <- 1
				return g, nil
			}
			return nil, errors.New("create grouping error")
		}
	}
	return nil, errors.New("not ready to create grouping ")
}

// CreateGrouping func
func CreateGrouping(name, field string) group.IGrouping {
	fmt.Println("create grouping !!!!!!!!!", name)
	gstorm := GetGstorm()

	for _, f := range gstorm.Factorys {
		g := f.CreateGrouping(name, field)
		fmt.Println("create grouping0000000000", reflect.TypeOf(g))
		if g != nil {
			return g
		}
	}
	return nil
}

// WatchNodeReal func
func WatchNodeReal(name string, worker *Worker) {
	gstorm := GetGstorm()
	node := worker.Next
	fmt.Println("watch node++++++++++++++++++++------------------**************", worker.Next, node)

	num, err := GetParallelNumber(name, node)
	if err != nil {
		fmt.Println(err)
		return
	}

	key := "/real/" + name + "/" + node + "/"
	can, err1 := CreateWorker(key, num, worker)
	if err1 != nil {
		return
	}
	if can {
		return
	}

	rch := gstorm.EtcdClient.Etcd.Watch(context.Background(), key, clientv3.WithPrefix())

	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			switch ev.Type {
			case mvccpb.PUT:
				//key = "/real/" + name + "/" + node + "/"
				can1, err2 := CreateWorker(key, num, worker)
				if err2 != nil {
					return
				}
				if can1 {
					return
				}

				break
			case mvccpb.DELETE:
				//t.Name.Remove(key1)
				//delete(t.groupings, key1)
			}
		}
	}

}

//CreateNodes func
func CreateNodes(worker *Worker, nextNodes []string) IHandle {
	fmt.Println("Create nodes", worker, nextNodes)
	gstorm := GetGstorm()
	workers := gstorm.WatchNodes[worker.NodeName]
	var chans []chan tuple.IID
	for _, v := range nextNodes {
		var sender send.ISender
		fmt.Println("create nodes++++++++++++++", worker.NType, v)
		if worker.NType == "bolt" || worker.NType == "Spout" {
			sender = send.NewRpcSender(v)

			sender.Prepare()
			chans = append(chans, sender.GetOutchan())
			go sender.Run()
		}
	}

	for _, v := range workers {
		CreateOneNode(v, chans)
	}

	return nil
}

// CreateReciever func
func CreateReciever(worker *Worker, node IHandle) {
	gstorm := GetGstorm()
	key := "/real/" + gstorm.TopologyName + "/" + worker.NodeName + "/" + worker.Name
	addr, err2 := GetAddr(gstorm.ServerName)
	if err2 != nil {
		fmt.Println(err2)
		return
	}
	var r send.IReciever
	gstorm.EtcdClient.Set(key, addr)
	if worker.NType == "acker" {
		fmt.Println("create acker reciever00000000000000000000000000000 ", addr)
		r = NewAckerReciever(addr)
	} else {
		r = send.NewReciever(addr)
	}

	go r.ListenAndServe(node.GetInchan())
}

// CreateAckerResultReciever func
func CreateAckerResultReciever(worker *Worker, node IHandle) *AckerResultReciever {
	gstorm := GetGstorm()
	//key := "/real/" + storm.TopologyName + "/" + worker.NodeName + "/" + worker.Name
	addr, err2 := GetAddr(gstorm.Name)
	if err2 != nil {
		fmt.Println(err2)
		return nil
	}
	//gstorm.EtcdClient.Set(key, addr)
	r := NewAckerResultReciever(addr)
	go r.ListenAndServe(node.GetInchan())
	return r
}

// CreateOneNode func
func CreateOneNode(worker *Worker, chans []chan tuple.IID) {
	gstorm := GetGstorm()
	node := CreateNodeFactory(worker.NodeName, gstorm.ServerName, worker.Name)
	fmt.Println("create node------:", worker.NodeName, reflect.TypeOf(node))

	node.SetSerial(gstorm.Serial)

	gstorm.CreatedWorkers = append(gstorm.CreatedWorkers, node)

	if worker.NType == "node" {
		node.SetSerial(gstorm.Serial)
	}
	if !worker.Start {
		CreateReciever(worker, node)
	} else {
		r := CreateAckerResultReciever(worker, node)

		if s, ok := node.(ISpout); ok {
			r.SetQueue(s.GetQueue())
			s.SetAckerReciever(r)
		}
	}

	if worker.Grouping != "null" {
		g := CreateGrouping(worker.Grouping, worker.Field)
		if g != nil {
			g.Prepare(chans)
			g.Launch()
			node.SetGrouping(g)
		}
	}
	bolt, ok1 := node.(IBolt)
	if ok1 {
		bolt.Prepare()
		go bolt.Run()
		return
	}
	fmt.Println("node type is ", reflect.TypeOf(node))
	value, ok := node.(ISpout)
	if ok {
		gstorm.Starts = append(gstorm.Starts, value)
		fmt.Println("start worker", len(gstorm.Starts), len(gstorm.StartWorkers), gstorm.Starts, gstorm.StartWorkers)
		if len(gstorm.Starts) == len(gstorm.StartWorkers) {
			Start()
		}
	}
}

// Start func
func Start() {
	fmt.Println("start topology!!!")
	gstorm := GetGstorm()
	for _, node := range gstorm.CreatedWorkers {
		fmt.Println("set acker group", reflect.TypeOf(node))
		node.SetAckerGrouping(gstorm.AckerGrouping)
	}

	for _, node := range gstorm.Starts {
		files := make(map[string]string)
		str, _ := user.Current()
		fmt.Println(str)
		files["1"] = str.HomeDir + "/my.txt"
		node.Open(files)
		go node.Run()
		node.Launch()
	}
}

// GetAddr name mechine name
func GetAddr(server string) (string, error) {
	gstorm := GetGstorm()
	ip, err := data.GetLocalIP()
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	p := data.GetSingletonPort()
	port, err1 := p.GetLocalPort(name, gstorm.EtcdClient)
	if err1 != nil {
		fmt.Println(err1)
		return "", err1
	}
	addr := ip + ":" + strconv.Itoa(port)
	return addr, nil
}

// CreateNodeFactory func
func CreateNodeFactory(node, server, name string) IHandle {
	gstorm := GetGstorm()

	for _, f := range gstorm.Factorys {
		n := f.CreateNode(node, server, name)
		if n != nil {
			return n
		}
	}
	return nil
}
