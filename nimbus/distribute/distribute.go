package distribute

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/gstormlee/gstorm/core/etcd"

	simplejson "github.com/bitly/go-simplejson"
)

// NodeData struct
type NodeData struct {
	Name     string
	Num      int
	Next     string
	Grouping string
	index    int
	NType    string
	Field    string
}

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

// TopologyData struct
type TopologyData struct {
	Name   string
	Begin  string
	Spouts []*NodeData
	Bolts  map[string]*NodeData
	Acker  *NodeData
}

// GetSpout func
func (t *TopologyData) GetSpout(name string) *NodeData {
	for _, s := range t.Spouts {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// GetBolt func
func (t *TopologyData) GetBolt(name string) *NodeData {
	for _, s := range t.Bolts {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// GetSpouts func
func (t *TopologyData) GetSpouts() []*NodeData {
	return t.Spouts
}

// SetSpouts func
func (t *TopologyData) SetSpouts(m []*NodeData) {
	t.Spouts = m
}

// GetBolts func
func (t *TopologyData) GetBolts() map[string]*NodeData {
	return t.Bolts
}

// NewTopologyData func
func NewTopologyData() *TopologyData {
	return &TopologyData{Bolts: make(map[string]*NodeData)}
}

// ReadJSON func
func ReadJSON(file string, storm string) error {
	topologyData := NewTopologyData()
	fmt.Println(file)
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
		return err
	}
	js, err := simplejson.NewJson(bytes)
	if err != nil {
		fmt.Println("read json error ", err)
		panic(err.Error())
	}
	//next := js.Get("start").MustString()
	name := js.Get("name").MustString()
	Spouts, err1 := js.Get("Spout").Array()
	if err1 != nil {
		fmt.Println(err)
		return err1
	}
	var ss []*NodeData
	//topologyData.SetSpouts(list)
	var next string
	for i := range Spouts {
		Spout := &NodeData{}
		//Spout.Name = next

		s := js.Get("Spout").GetIndex(i)
		n := s.Get("name").MustString()
		fmt.Println("Spout name", n)
		Spout.Name = n
		g := s.Get("grouping").MustString()
		Spout.Grouping = g

		ne := s.Get("next").MustString()
		Spout.Next = ne
		next = ne
		Spout.Num = s.Get("num").MustInt()
		Spout.NType = s.Get("ntype").MustString()

		ss = append(ss, Spout)
	}
	topologyData.SetSpouts(ss)
	for {
		node := GetNodeFromJSON(next, js)
		node.Name = next
		next = node.Next
		topologyData.Bolts[node.Name] = node
		if next == "end" {
			break
		}
	}
	node := GetNodeFromJSON("acker", js)
	fmt.Println("acker node", node, node.Num, node.Name)
	topologyData.Acker = node
	data := GetInstance()
	data.Datas[name] = topologyData
	WriteToplogy(name, storm)
	return nil
}

// GetNodeFromJSON func
func GetNodeFromJSON(name string, js *simplejson.Json) *NodeData {
	bolt := &NodeData{}
	bolt.Num = js.Get(name).Get("num").MustInt()
	bolt.Next = js.Get(name).Get("next").MustString()
	bolt.Grouping = js.Get(name).Get("grouping").MustString()
	bolt.NType = js.Get(name).Get("ntype").MustString()
	bolt.Field = js.Get(name).Get("field").MustString()
	bolt.Name = js.Get(name).Get("name").MustString()
	return bolt
}

// WriteToplogy func
func WriteToplogy(name string, storm string) {
	instance := GetInstance()
	client := etcd.NewClient(instance.EtcdAddr)

	key := "/topology/" + name
	client.DeleteWithPreFix(key)
	key2 := "/real/" + name
	client.DeleteWithPreFix(key2)
	key3 := "/distribute/" + name
	client.DeleteWithPreFix(key3)
	//client.Set(key, name)
	key1 := key + "/file"
	fmt.Println(key1, storm)
	client.Set(key1, storm)
	key1 = key + "/start"
	Spouts := instance.Datas[name].GetSpouts()
	fmt.Println("Spouts", Spouts)
	b, err2 := json.Marshal(Spouts)
	fmt.Println("json", string(b[:]))
	if err2 == nil {
		client.Set(key1, string(b[:]))
	}
	bolts := instance.Datas[name].Bolts
	key1 = key // + "/bolts"

	for _, v := range bolts {
		key2 = key1 + "/" + v.Name

		key3 = key2 + "/next"
		client.Set(key3, v.Next)
		key3 = key2 + "/num"
		str := strconv.Itoa(v.Num)
		client.Set(key3, str)
		key3 = key2 + "/grouping"
		client.Set(key3, v.Grouping)
	}

	acker := instance.Datas[name].Acker
	fmt.Println("acker", acker)
	key2 = key1 + "/" + acker.Name
	key3 = key2 + "/next"
	client.Set(key2, acker.Next)
	key3 = key2 + "/num"
	str := strconv.Itoa(acker.Num)
	client.Set(key3, str)
	key3 = key2 + "/grouping"
	client.Set(key3, acker.Grouping)
	fmt.Println("want to call distribute")
	Distribute(name)
}

// Distribute func
func Distribute(name string) error {
	data := GetInstance()

	Spouts := data.Datas[name].GetSpouts()

	i := 1
	var Nodes []Worker
	for _, Spout := range Spouts {
		for j := 1; j <= Spout.Num; j++ {
			str := strconv.Itoa(j)
			worker := Worker{}
			worker.Name = Spout.Name + str
			worker.NodeName = Spout.Name
			worker.Next = Spout.Next
			worker.Index = i
			worker.Grouping = Spout.Grouping
			worker.Field = Spout.Field
			worker.NType = Spout.NType
			worker.Start = true
			Nodes = append(Nodes, worker)
			fmt.Println("worker", worker)
			i++
		}
	}
	bolts := data.Datas[name].Bolts
	fmt.Println("worker count", len(bolts))
	for _, v := range bolts {
		for j := 1; j <= v.Num; j++ {
			str := strconv.Itoa(j)
			worker := Worker{}
			worker.Name = v.Name + str
			worker.NodeName = v.Name
			worker.Index = i
			worker.Next = v.Next
			worker.Grouping = v.Grouping
			worker.Start = false
			worker.Field = v.Field
			worker.NType = v.NType
			fmt.Println("worker", worker)
			Nodes = append(Nodes, worker)
			i++
		}
	}
	if node := data.Datas[name].Acker; node != nil {
		fmt.Println("acker number:", node.Num)
		for j := 1; j <= node.Num; j++ {
			str := strconv.Itoa(j)
			worker := Worker{}
			worker.Name = node.Name + str
			worker.NodeName = node.Name
			worker.Next = node.Next
			worker.Grouping = node.Grouping
			worker.Start = false
			worker.Field = node.Field
			worker.NType = node.NType
			worker.Index = i
			i++
			Nodes = append(Nodes, worker)
		}
	}

	Supervisors := data.Supervisors.List()
	count := len(Supervisors)
	fmt.Println("count", len(Nodes), count)
	for _, node := range Nodes {
		num := node.Index % count
		supervisor := Supervisors[num]
		key := "/distribute/" + name + "/" + supervisor + "/" + node.NodeName + "/" + node.Name
		b, err := json.Marshal(node)
		if err != nil {
			return err
		}
		data.EtcdClient.Set(key, string(b[:]))
	}
	fmt.Println("delete key")
	key := "/serial/" + name

	data.EtcdClient.Set(key, "1")
	key = "/topologyname/" + name
	data.EtcdClient.Set(key, "distribute")
	fmt.Println("end distribute")
	return nil
}
