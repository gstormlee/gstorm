package topology

import (
	"strconv"

	"github.com/gstormlee/gstorm/core/tuple"
)

// TopologyData struct
type JSONFileData struct {
	Name     string  `json:"name"`
	NodeName string  `json:"nodename, omitempty"`
	Spouts   []*Node `json:"spouts"`
	Bolts    []*Node `json:"bolts, omitempty"`
	Acker    *Node   `json:"ackers,omitempty"`
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

// NewTopologyData func
func NewTopologyData() *JSONFileData {
	return &JSONFileData{}
}

// NewNode func
func NewNode(node *Node, i int) *Node {
	n := &Node{}
	n.Name = node.Name
	n.NType = node.NType
	n.Num = node.Num
	n.MasterGrouping = node.MasterGrouping
	//n.Grouping = node.Grouping
	n.NodeName = node.Name + strconv.Itoa(i)
	return n
}

// MasterGroupingData struct
type MasterGroupingData struct {
	Next      []string       `json:"next"`
	Field     string         `json:"field,omitempty"`
	GType     string         `json:"type"`
	Groupings []GroupingData `json:"groupings"`
}

// GetGroupingData func
func (mg *MasterGroupingData) GetGroupingData(next string) *GroupingData {
	for _, v := range mg.Groupings {
		if next == v.Next {
			return &v
		}
	}
	return nil
}

// GroupingData struct
type GroupingData struct {
	Next  string `json:"next"`
	Field string `json:"field,omitempty"`
	GType string `json:"type"`
}

// Node struct
type Node struct {
	NodeName       string              `json:"nodename, omitempty"`
	Name           string              `json:"name"`
	NType          string              `json:"type"`
	Num            int                 `json:"num"`
	MasterGrouping *MasterGroupingData `json:"mastergrouping,omitempty"`
}

// GetNext func
func (n *Node) GetNext() []string {
	if n.MasterGrouping == nil {
		return nil
	}

	return n.MasterGrouping.Next
}

// WatchWorker struct
type WatchWorker struct {
	Name2Node map[string][]*Node
	Node2Name map[*Node][]string
	Ready     []string
	NextChans map[string][]chan tuple.IID
}

// NewWatchWorker func
func NewWatchWorker() WatchWorker {
	w := WatchWorker{}
	w.Name2Node = make(map[string][]*Node)
	w.Node2Name = make(map[*Node][]string)
	w.NextChans = make(map[string][]chan tuple.IID)
	return w
}

// IsReady func
func (w *WatchWorker) IsReady(name []string) bool {
	for _, v := range name {
		have := false
		for _, n := range w.Ready {
			if n == v {
				have = true
				break
			}
		}
		if !have {
			return false
		}
	}

	return true
}
