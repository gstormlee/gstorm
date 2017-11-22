package group

import (
	"math/rand"

	"github.com/gstormlee/gstorm/core/tuple"
)

// ShuffleGrouping struct

type ShuffleGrouping struct {
	Grouping
}

// NewShuffleGrouping func
func NewShuffleGrouping() *ShuffleGrouping {
	out := &ShuffleGrouping{}
	g := NewGrouping()
	out.Grouping = *g
	//out.inchan = make(chan tuple.IID, 10)
	return out
}

// Prepare func
func (g *ShuffleGrouping) Prepare(out []chan tuple.IID) {
	g.Grouping.OutChan = out
}

// Run func
func (g *ShuffleGrouping) Run() {
	for {
		data := <-g.inChan
		idx := rand.Int31n(int32(len(g.OutChan)))
		g.OutChan[idx] <- data
	}
}

// Launch func
func (g *ShuffleGrouping) Launch() {
	go g.Run()
}

// Tuple func
func (g *ShuffleGrouping) Tuple(data tuple.IID) {
	g.inChan <- data
}
