package main

import "github.com/gstormlee/gstorm/core/topology"
import "github.com/gstormlee/gstorm/core/tuple"

type PrintBolt struct {
	topology.Handle
}

//NewPrintBolt create a NewPrintBolt instance
func NewPrintBolt(name, node string) *PrintBolt {
	bolt := &PrintBolt{}
	handle := topology.NewHandle(node)
	bolt.Handle = *handle
	return bolt
}

//Execute func
func (b *PrintBolt) Execute(data tuple.IID) {
	dat, ok := data.(*tuple.NsqType)
	println(ok)
	if ok {
		println(dat.Msg)
	}
}
