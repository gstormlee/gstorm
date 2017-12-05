package send

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/smallnest/rpcx"
)

// IClient interface
type IClient interface {
	Connect() error
	Send(data tuple.IID)
	Dial() error
	SetAddr(addr string)
	GetAddr() string
	SetRpcxClient(client *rpcx.Client)
	GetRpcxClient() *rpcx.Client
}

// Client struct
type Client struct {
	Addr   string
	Client *rpcx.Client
}

// NewClient func
func NewClient(addr string) *Client {
	c := new(Client)
	c.Addr = addr
	return c
}

// Connect func
func (c *Client) Connect() error {
	s := &rpcx.DirectClientSelector{Network: "tcp", Address: c.GetAddr(), DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(s)

	c.Client = client

	return nil
}

// Send func
func (c *Client) Send(data tuple.IID) {
	var r Replay

	if b, err := json.Marshal(data); err == nil {
		d := Message{}
		a := strings.Split(reflect.TypeOf(data).String(), ".")
		str := a[len(a)-1]
		d.DataType = str
		d.Data = string(b[:])
		err := c.Client.Call(context.Background(), "Queue.PushData", d, &r)
		fmt.Println(err)
	}
	// if _, ok := data.(*tuple.SentenceValue); ok {

	// } else if d1, ok1 := data.(*tuple.WordValue); ok1 {
	// 	c.Client.Call(context.Background(), "Queue.PushWord", d1, &r)
	// } else if d2, ok2 := data.(*tuple.NsqType); ok2 {
	// 	c.Client.Call(context.Background(), "Queue.PushNsq", d2, &r)
	// }

}

// Dial func
func (c *Client) Dial() error {
	s := &rpcx.DirectClientSelector{Network: "tcp", Address: c.Addr, DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(s)

	c.Client = client

	return nil
}

// SetAddr func
func (c *Client) SetAddr(addr string) {
	c.Addr = addr
}

// GetAddr func
func (c *Client) GetAddr() string {
	return c.Addr
}

// GetRpcxClient func
func (c *Client) GetRpcxClient() *rpcx.Client {
	return c.Client
}

// SetRpcxClient func
func (c *Client) SetRpcxClient(client *rpcx.Client) {
	c.Client = client
}
