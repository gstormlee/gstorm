package topology

import (
	"github.com/gstormlee/gstorm/core/topology/group"
)

type IFactory interface {
	CreateNode(node, server, name string) IHandle
	CreateGrouping(name string, field string) group.IGrouping
}

// Factory struct
type Factory struct {
}

// CreateNode func
func (f *Factory) CreateNode(node, server, name string) IHandle {
	switch node {
	case "AckerBolt":
		return NewAckerBolt(server, name)
	default:
		return nil
	}

}

// CreateGrouping func
func (f *Factory) CreateGrouping(name string, field string) group.IGrouping {
	switch name {
	case "FieldGrouping":
		g := group.NewFieldGrouping(field)
		return g
	case "ShuffleGrouping":
		g := group.NewShuffleGrouping()
		return g
	default:
		return nil
	}
}
