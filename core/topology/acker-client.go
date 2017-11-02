package topology

import (
	"context"
	"time"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/gstormlee/gstorm/core/send"

	"github.com/smallnest/rpcx"
)

type AckerClient struct {
	Addr   string
	Client *rpcx.Client
}

func NewAckerClient(addr string) *AckerClient {

	c := new(AckerClient)
	c.Addr = addr

	return c
}

// Connect func
func (c *AckerClient) Connect() error {
	s := &rpcx.DirectClientSelector{Network: "tcp", Address: c.GetAddr(), DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(s)

	c.Client = client

	return nil
}

// SetAddr func
func (c *AckerClient) SetAddr(addr string) {
	c.Addr = addr
}

// GetAddr func
func (c *AckerClient) GetAddr() string {
	return c.Addr
}

// GetRpcxClient func
func (c *AckerClient) GetRpcxClient() *rpcx.Client {
	return c.Client
}

// SetRpcxClient func
func (c *AckerClient) SetRpcxClient(client *rpcx.Client) {
	c.Client = client
}

// Send func
func (c *AckerClient) Send(data tuple.IID) {
	var r send.Replay
	if d, ok := data.(*AckerBegin); ok {
		c.Client.Call(context.Background(), "AckerOp.Begin", d, &r)
	} else if d1, ok1 := data.(*Acker); ok1 {
		c.Client.Call(context.Background(), "AckerOp.Acker", d1, &r)
	} else if d3, ok3 := data.(*AckerResult); ok3 {
		c.Client.Call(context.Background(), "AckerOp.Finish", d3, &r)
	}

}
