package topology

import (
	"github.com/gstormlee/gstorm/core/tuple"
)

// IBolt interface
type IBolt interface {
	Prepare()
	Run()
	Execute(data tuple.IID)
	//DeclareOutputFields() *Fields
}

// Bolt struct
type Bolt struct {
	Handle
}

// NewBolt name machine name
func NewBolt(name, node string) *Bolt {
	bolt := &Bolt{}
	bolt.Name = node
	bolt.Inchan = make(chan tuple.IID)
	h := NewHandle(node)
	bolt.Handle = *h
	return bolt
}

// Prepare func
func (b *Bolt) Prepare() {
}

// Run func
func (b *Bolt) Run() {
	for {
		data := <-b.Inchan
		b.Execute(data)
	}
}

// Execute func
func (b *Bolt) Execute(data tuple.IID) {
}
