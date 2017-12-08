package distribute

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/gstormlee/gstorm/core/etcd"
)

// JSONFileData struct
type JSONFileData struct {
	Name     string  `json:"name"`
	NodeName string  `json:"nodename, omitempty"`
	Spouts   []*Node `json:"spouts"`
	Bolts    []*Node `json:"bolts, omitempty"`
	Acker    *Node   `json:"acker,omitempty"`
	Config   *Config `json:"config, omitempty"`
}

// Config struct
type Config struct {
	AckerBoltTimeOut int
}

// GetSpout func
func (t *JSONFileData) GetSpout(name string) *Node {
	for _, s := range t.Spouts {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// GetBolt func
func (t *JSONFileData) GetBolt(name string) *Node {
	for _, s := range t.Bolts {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// GetSpouts func
func (t *JSONFileData) GetSpouts() []*Node {
	return t.Spouts
}

// SetSpouts func
func (t *JSONFileData) SetSpouts(m []*Node) {
	t.Spouts = m
}

// GetBolts func
func (t *JSONFileData) GetBolts() []*Node {
	return t.Bolts
}

// NewJSONFileData func
func NewJSONFileData() *JSONFileData {
	return &JSONFileData{}
}

// ReadTopology func
func ReadTopology(file string) (*JSONFileData, error) {
	topologyData := NewJSONFileData()
	fmt.Println(file)
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
		return nil, err
	}
	if err2 := json.Unmarshal(bytes, topologyData); err2 == nil {
		data := GetInstance()
		s := data.Datas[file]
		delete(data.Datas, file)

		fmt.Println("acker", topologyData.Acker)

		s.Data = topologyData

		data.Datas[topologyData.Name] = s
		topologyData.WriteTopology(topologyData.Name, s.Bin)
	} else {
		fmt.Println("read file ", err2)
		return nil, err2
	}
	return nil, nil
}

// WriteToplogy func
func (t *JSONFileData) WriteTopology(topology string, storm string) {
	instance := GetInstance()
	client := etcd.NewClient(instance.EtcdAddr)
	key := "/topology/" + t.Name
	client.DeleteWithPreFix(key)
	key2 := "/real/" + t.Name
	client.DeleteWithPreFix(key2)
	key3 := "/distribute/" + t.Name
	client.DeleteWithPreFix(key3)
	key1 := key + "/file"
	client.Set(key1, storm)
	key1 = key + "/spouts/"
	Spouts := t.Spouts
	for _, v := range Spouts {
		k := key1 + v.Name
		if b, err2 := json.Marshal(v); err2 == nil {
			client.Set(k, string(b[:]))
		}
	}

	bolts := t.Bolts
	key1 = key + "/bolts/"
	for _, v := range bolts {
		k := key1 + v.Name
		if b, err2 := json.Marshal(v); err2 == nil {
			client.Set(k, string(b[:]))
		}
	}

	key1 = key + "/ackers/"

	if t.Acker != nil {
		k := key1 + t.Acker.Name
		if b, err2 := json.Marshal(t.Acker); err2 == nil {
			client.Set(k, string(b[:]))
		}
	}

	t.Distribute()
}

// Distribute func
func (JSON *JSONFileData) Distribute() error {
	data := GetInstance()
	var nodes []*Node
	for _, spout := range JSON.Spouts {
		for j := 1; j <= spout.Num; j++ {
			node := NewNode(spout, j)
			nodes = append(nodes, node)
			fmt.Println("worker", node)
		}
	}
	bolts := JSON.Bolts
	fmt.Println("worker count", len(bolts))
	for _, bolt := range bolts {
		for j := 1; j <= bolt.Num; j++ {
			node := NewNode(bolt, j)
			nodes = append(nodes, node)
		}
	}
	if acker := JSON.Acker; acker != nil {
		for j := 1; j <= acker.Num; j++ {
			node := NewNode(acker, j)
			nodes = append(nodes, node)
		}
	}

	Supervisors := data.Supervisors.List()
	count := len(Supervisors)
	i := 0
	for _, node := range nodes {
		i++
		num := i % count
		supervisor := Supervisors[num]
		key := "/distribute/" + JSON.Name + "/" + supervisor + "/" + node.NodeName + "/" + node.Name
		b, err := json.Marshal(node)
		if err != nil {
			return err
		}
		data.EtcdClient.Set(key, string(b[:]))
	}
	key := "/serial/" + JSON.Name
	data.EtcdClient.Set(key, "1")
	key = "/topologyname/" + JSON.Name
	data.EtcdClient.Set(key, "distribute")
	key = "/topology/" + JSON.Name

	return nil
}
