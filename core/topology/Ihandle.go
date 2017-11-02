package topology

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/gstormlee/gstorm/core/topology/group"
)

// IHandle interface
type IHandle interface {
	GetName() string
	Emmitter(data tuple.IID)
	GenerateID() string
	GetInchan() chan tuple.IID
	SetGrouping(g group.IGrouping)
	GetGrouping() group.IGrouping
	SetSerial(serial int)
	GetSerial() int
	SetAddr(addr string)
	SetAckerGrouping(g group.IGrouping)
}

// Handle struct
type Handle struct {
	Name   string
	Serial int

	Grouping       group.IGrouping
	Base           int
	Lock           sync.Mutex
	Inchan         chan tuple.IID
	Addr           string
	TupleCollector *Collector
	Time           string
}

// NewHandle func
func NewHandle(name string) *Handle {
	handle := &Handle{}
	handle.Name = name
	handle.Inchan = make(chan tuple.IID, 10)
	handle.TupleCollector = NewCollector()
	return handle
}

// GenerateID Func
func (h *Handle) GenerateID() string {
	h.Lock.Lock()
	str := strconv.Itoa(h.GetSerial())
	if h.Base == 0 || h.Base == 9999 {
		t := time.Now().Unix()
		fmt.Println(t)
		str += strconv.FormatInt(t, 10)
		h.Time = str
	} else {
		str += h.Time
	}
	h.Base++ //i = AddInt32(i, 1)
	str1 := fmt.Sprintf("%04d", h.Base)
	str += str1
	h.Lock.Unlock()
	return str
}

// Emmitter func
func (h *Handle) Emmitter(data tuple.IID) {
	data1, ok := data.(tuple.IData)
	if ok {
		h.TupleCollector.SetLast(data.GetID(), data1.GetCurrentID())
		id := h.GenerateID()
		data1.SetCurrentID(id)
		if h.Grouping != nil {
			h.Grouping.Tuple(data1)
		}
	} else {
		fmt.Println("data to IData error")
	}
}

// GetName func
func (h *Handle) GetName() string {
	return h.Name
}

// GetInchan Func
func (h *Handle) GetInchan() chan tuple.IID {
	return h.Inchan
}

// SetGrouping func
func (h *Handle) SetGrouping(g group.IGrouping) {
	h.Grouping = g
}

// GetGrouping func
func (h *Handle) GetGrouping() group.IGrouping {
	return h.Grouping
}

// GetSerial func
func (h *Handle) GetSerial() int {
	return h.Serial
}

// SetSerial func
func (h *Handle) SetSerial(serial int) {
	h.Serial = serial
}

// SetAddr func
func (h *Handle) SetAddr(addr string) {
	h.Addr = addr
	h.TupleCollector.SetAddr(addr)
}

// SetAckerGrouping func
func (h *Handle) SetAckerGrouping(g group.IGrouping) {
	h.TupleCollector.SetGrouping(g)
}
