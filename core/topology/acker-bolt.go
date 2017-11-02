package topology

import (
	"fmt"
	"strconv"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/gstormlee/gstorm/core/send"
)

// AckerBolt struct
type AckerBolt struct {
	Handle
	Ackers  map[string]*AckerData
	senders map[string]AckerSenderData
}

// AckerSenderData struct
type AckerSenderData struct {
	sender send.ISender
	queue  chan tuple.IID
}

// NewAckerBolt name machine name
func NewAckerBolt(name, node string) *AckerBolt {
	bolt := &AckerBolt{}
	h := NewHandle(name)
	bolt.Handle = *h
	bolt.Ackers = make(map[string]*AckerData)
	bolt.senders = make(map[string]AckerSenderData)
	return bolt
}

// Prepare func
func (b *AckerBolt) Prepare() {
	fmt.Println("bolt prepare")
}

// Run func
func (b *AckerBolt) Run() {
	for {

		data := <-b.Inchan
		b.Execute(data)
	}
}

// Execute func
func (b *AckerBolt) Execute(data tuple.IID) {
	if d, ok := data.(*AckerBegin); ok {
		iid, err := strconv.ParseInt(d.CurrentID, 10, 64)
		if err != nil {
			fmt.Println(err)
		} else {
			acker := NewAckerData(d.GetID(), d.Addr)
			acker.Result = iid
			b.Ackers[d.GetID()] = acker
		}
	} else if d1, ok1 := data.(*Acker); ok1 {
		if v, ok := b.Ackers[d1.GetID()]; ok {
			val := v.Result ^ d1.Result
			b.Ackers[d1.GetID()].Result = val
			if val == 0 {
				fmt.Println("--------------------消息处理完成-------------------", d1.GetID())
				d2 := NewAckerResult(data.GetID(), Succeeded)
				b.SendData(v.Addr, d2)
			}
		}
	} else if _, ok2 := data.(*AckerResult); ok2 {
		if v, ok := b.Ackers[d1.GetID()]; ok {
			b.SendData(v.Addr, data)
			delete(b.Ackers, data.GetID())
		}
	}
}

// AddSender func
func (b *AckerBolt) AddSender(addr string) {
	sender := NewAckerSender(addr)
	sender.Prepare()
	go sender.Run()
	s := AckerSenderData{}
	s.sender = sender
	s.queue = sender.GetOutchan()
	b.senders[addr] = s
}

// SendData func
func (b *AckerBolt) SendData(addr string, data tuple.IID) {
	if _, ok1 := b.senders[addr]; !ok1 {
		b.AddSender(addr)
	}
	b.senders[addr].queue <- data
}
