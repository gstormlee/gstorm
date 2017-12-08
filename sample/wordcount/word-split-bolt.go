package main

import (
	"strings"
	"unicode"

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
	d, ok := data.(*SentenceValue)
	if ok {
		w.TupleCollector.SetLast(data.GetID(), d.GetCurrentID())
		f := func(c rune) bool {
			return !unicode.IsLetter(c) && !unicode.IsNumber(c)
		}

		words := strings.FieldsFunc(d.Sentence, f)

		for _, word := range words {

			word1 := &WordValue{}
			word1.Word = word
			word1.ID = tuple.ID{}
			word1.ID.ID = data.GetID()

			word1.CurrentID = "0"
			w.Emmitter(word1)
			w.TupleCollector.Acker(word1)
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
