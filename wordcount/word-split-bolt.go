package main

import (
	"fmt"
	"strings"

	"github.com/gstormlee/gstorm/core/topology"
	"github.com/gstormlee/gstorm/core/tuple"
)

// WordSplitBolt struct
type WordSplitBolt struct {
	//topology.Bolt
	topology.Handle
}

// NewWordSplitBolt func
func NewWordSplitBolt(name, node string) *WordSplitBolt {
	w := &WordSplitBolt{}
	handle := topology.NewHandle(node)
	w.Handle = *handle
	return w
}

// Execute func
func (w *WordSplitBolt) Execute(data tuple.IID) {
	d, ok := data.(*tuple.SentenceValue)
	if ok {
		a := strings.Split(d.Sentence, " ")
		for _, word := range a {
			isword := true
			for _, c := range word {

				if (c <= 'a' && c >= 'z') || (c <= 'A' && c >= 'Z') {
					isword = false
				}
			}
			if isword {
				word1 := &tuple.WordValue{}
				word1.Word = word
				word1.ID = tuple.ID{}
				word1.ID.ID = data.GetID()

				word1.CurrentID = d.GetCurrentID()
				w.TupleCollector.SetLast(word1.GetID(), word1.GetCurrentID())
				w.Emmitter(word1)

				w.TupleCollector.Acker(word1)
			}
		}
	}
}

// Run func
func (w *WordSplitBolt) Run() {

	for {
		data := <-w.Inchan
		w.Execute(data)
	}
}

func (w *WordSplitBolt) Prepare() {
}
