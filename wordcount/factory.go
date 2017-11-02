package main

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/topology/group"

	"github.com/gstormlee/gstorm/core/topology"
)

type Factory struct {
}

// Factory func
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
func (f *Factory) CreateGrouping(name string, field string) group.IGrouping {
	return nil
}
