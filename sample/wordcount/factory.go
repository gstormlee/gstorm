package main

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/topology"
	"github.com/gstormlee/gstorm/core/topology/group"
)

// Factory struct
type Factory struct {
}

// CreateNode func
func (f *Factory) CreateNode(node, server, name string) topology.IHandle {
	switch node {
	case "WordCountBolt":
		return NewWordCountBolt(server, name)
	case "SentenceSpout":
		return NewSentenceSpout(server, name)
	case "WordSplitBolt":
		return NewWordSplitBolt(server, name)

	default:
		fmt.Println("default")
		return nil
	}
}

// CreateGrouping func
func (f *Factory) CreateGrouping(name, field string) group.IGrouping {
	return nil
}

// CreateMasterGrouping func
func (f *Factory) CreateMasterGrouping(name, field string) group.IMasterGrouping {
	return nil
}
