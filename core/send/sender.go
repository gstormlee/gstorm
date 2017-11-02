package send

import (
	"time"

	"github.com/gstormlee/gstorm/core/tuple"
	"github.com/smallnest/rpcx"
)

type ISender interface {
	GetOutchan() chan tuple.IID
	Prepare()
	Run()
	Send(data tuple.IID)
}

// RpcSender struct
type RpcSender struct {
	Addr    string // ip and port
	Outchan chan tuple.IID
	Client  IClient
}

// NewRpcSender func
func NewRpcSender(addr string) *RpcSender {
	sender := new(RpcSender)
	sender.Addr = addr
	sender.Outchan = make(chan tuple.IID, 10)

	return sender
}

// GetOutchan func
func (s *RpcSender) GetOutchan() chan tuple.IID {
	return s.Outchan
}

// Prepare func
func (s *RpcSender) Prepare() {
	svr := &rpcx.DirectClientSelector{Network: "tcp", Address: s.Addr, DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(svr)

	s.Client = NewClient(s.Addr)
	s.Client.SetRpcxClient(client)
	return
}

// Run func
func (s *RpcSender) Run() {
	for {
		data := <-s.Outchan
		s.Send(data)
	}
}

// Send func
func (s *RpcSender) Send(data tuple.IID) {
	s.Client.Send(data)
}
