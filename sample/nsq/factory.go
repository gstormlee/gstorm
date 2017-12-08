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
	case "PrintBolt":
		return NewPrintBolt(server, name)
	case "NsqSpout":
		return NewNsqSpout(server, name)
	default:
		fmt.Println("default")
		return nil
	}
}

// CreateGrouping func
func (f *Factory) CreateGrouping(name string, field string) group.IGrouping {
	return nil
}
