package distribute

import "strconv"

// NewNode func
func NewNode(node *Node, i int) *Node {
	n := &Node{}
	n.Name = node.Name
	n.NType = node.NType
	n.Num = node.Num
	n.MasterGrouping = node.MasterGrouping
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
	return n.MasterGrouping.Next
}

// StormData struct
type StormData struct {
	TopologyFile string
	Bin          string
	Data         *JSONFileData
}

// WatchWorker struct
type WatchWorker struct {
	Name2Node map[string][]*Node
	Node2Name map[*Node][]string
	Ready     []string
}

// NewWatchWorker func
func NewWatchWorker() WatchWorker {
	w := WatchWorker{}
	w.Name2Node = make(map[string][]*Node)
	w.Node2Name = make(map[*Node][]string)
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
