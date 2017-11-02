package group

import (
	"math/rand"

	"github.com/gstormlee/gstorm/core/tuple"
)

// ShuffleGrouping struct

type ShuffleGtouping struct {
	Grouping
}

// NewShuffleGrouping func
func NewShuffleGrouping() *ShuffleGtouping {
	out := &ShuffleGtouping{}
	g := NewGrouping()
	out.Grouping = *g
	//out.inchan = make(chan tuple.IID, 10)
	return out
}

// Prepare func
func (g *ShuffleGtouping) Prepare(out []chan tuple.IID) {
	g.Grouping.outchan = out
}

// Run func
func (g *ShuffleGtouping) Run() {
	for {
		data := <-g.inChan
		idx := rand.Int31n(int32(len(g.outchan)))
		g.outchan[idx] <- data
	}
}

// Launch func
func (g *ShuffleGtouping) Launch() {
	go g.Run()
}

// Tuple func
func (g *ShuffleGtouping) Tuple(data tuple.IID) {
	g.inChan <- data
}
