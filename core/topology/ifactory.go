package topology

import (

	"github.com/gstormlee/gstorm/core/topology/group"
)

// IFactory interface
type IFactory interface {
	CreateNode(node, server, name string) IHandle
	CreateGrouping(name string, field string) group.IGrouping
	CreateMasterGrouping(name, field string) group.IMasterGrouping
}

// Register func
func (t *Topology) Register(factory IFactory) {
	t.Factories.Factories = append(t.Factories.Factories, factory)
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

// CreateMasterGrouping func
func (f *Factory) CreateMasterGrouping(name, field string) group.IMasterGrouping {
	switch name {
	case "AllMasterGrouping":
		m := group.NewAllMasterGrouping()
		return m
	case "NullMasterGrouping":
	
		m := group.NewNullMasterGrouping()
		return m
	default:
		return nil
	}
}

// Factories struct
type Factories struct {
	Factories []IFactory
}

// CreateNodeFactory func
func (fs *Factories) CreateNodeFactory(node, server, name string) IHandle {
	for _, f := range fs.Factories {
		n := f.CreateNode(node, server, name)
		if n != nil {
			return n
		}
	}
	return nil
}

// CreateGrouping func
func (fs *Factories) CreateGrouping(g GroupingData) group.IGrouping {

	for _, f := range fs.Factories {
		g := f.CreateGrouping(g.GType, g.Field)
		if g != nil {
		
			return g
		}
	}
	return nil
}

// CreateMasterGrouping func
func (fs *Factories) CreateMasterGrouping(g *MasterGroupingData) group.IMasterGrouping {
	
	for _, f := range fs.Factories {
		mg := f.CreateMasterGrouping(g.GType, g.Field)
		if mg != nil {
			return mg
		}
	}
	return nil
}
