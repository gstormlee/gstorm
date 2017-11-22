package group

import (

	"github.com/gstormlee/gstorm/core/tuple"
)

// IGrouping interface
type IGrouping interface {
	Prepare(out []chan tuple.IID)
	Run()
	Launch()
	Tuple(t tuple.IID)
}

// Grouping struct
type Grouping struct {
	inChan  chan tuple.IID
	OutChan []chan tuple.IID
}

// NewGrouping func
func NewGrouping() *Grouping {
	g := &Grouping{}
	g.inChan = make(chan tuple.IID, 10)
	return g
}

// Prepare func
func (g *Grouping) Prepare(out []chan tuple.IID) {
	g.OutChan = out
}

// Run func
func (g *Grouping) Run() {
}

// Launch func
func (g *Grouping) Launch() {
	go g.Run()
}

// Tuple func
func (g *Grouping) Tuple(data tuple.IID) {

	g.inChan <- data
}
