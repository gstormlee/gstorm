package topology

import (
	"time"

	"github.com/gstormlee/gstorm/core/tuple"
	"github.com/smallnest/rpcx"
)

// AckerSender struct
type AckerSender struct {
	Addr    string // ip and port
	Outchan chan tuple.IID
	Client  *AckerClient
}

// NewAckerSender func
func NewAckerSender(addr string) *AckerSender {
	sender := new(AckerSender)
	sender.Addr = addr
	sender.Outchan = make(chan tuple.IID, 10)
	return sender
}

// GetOutchan func
func (s *AckerSender) GetOutchan() chan tuple.IID {
	return s.Outchan
}

// Prepare func
func (s *AckerSender) Prepare() {
	svr := &rpcx.DirectClientSelector{Network: "tcp", Address: s.Addr, DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(svr)

	c := NewAckerClient(s.Addr)
	s.Client = c
	s.Client.SetRpcxClient(client)
}

// Run func
func (s *AckerSender) Run() {
	for {
		data := <-s.Outchan
		s.Send(data)
	}
}

// Send func
func (s *AckerSender) Send(data tuple.IID) {
	s.Client.Send(data)
}
