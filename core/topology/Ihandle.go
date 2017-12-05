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
	GetInchan() chan tuple.IID
	SetMasterGrouping(g group.IMasterGrouping)
	GetMasterGrouping() group.IMasterGrouping
	SetAddr(addr string)
	SetAckerGrouping(g group.IGrouping)
}

var once sync.Once

type GlobalGenerator struct {
	Lock   sync.Mutex
	Base   int
	Time   string
	Serial int
}

var generator *GlobalGenerator

func GetGlobalGenerator() *GlobalGenerator {
	once.Do(func() {
		generator = &GlobalGenerator{}
	})
	return generator
}

// GetSerial func
func (g *GlobalGenerator) GetSerial() int {
	return g.Serial
}

// SetSerial func
func (g *GlobalGenerator) SetSerial(serial int) {
	g.Serial = serial
}

// Handle struct
type Handle struct {
	Name           string
	MasterGrouping group.IMasterGrouping
	Base           int
	Inchan         chan tuple.IID
	Addr           string
	TupleCollector *Collector
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
func (g *GlobalGenerator) GenerateID() string {
	g.Lock.Lock()
	str := strconv.Itoa(g.GetSerial())
	if g.Base == 0 || g.Base == 9999 {
		t := time.Now().Unix()
		g.Time = strconv.FormatInt(t, 10)
	}
	str += g.Time
	g.Base++ //i = AddInt32(i, 1)
	str1 := fmt.Sprintf("%04d", g.Base)
	str += str1
	g.Lock.Unlock()
	return str
}

// Emmitter func
func (h *Handle) Emmitter(data tuple.IID) {
	gen := GetGlobalGenerator()
	data1, ok := data.(tuple.IData)
	if ok {
		if len(data1.GetCurrentID()) > 0 && data1.GetCurrentID() != "0" {
			h.TupleCollector.SetLast(data.GetID(), data1.GetCurrentID())
		}
		data1.SetCurrentID(gen.GenerateID())
		if h.MasterGrouping != nil {
			h.MasterGrouping.Tuple(data1)
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
func (h *Handle) SetMasterGrouping(g group.IMasterGrouping) {
	h.MasterGrouping = g
}

// GetGrouping func
func (h *Handle) GetMasterGrouping() group.IMasterGrouping {
	return h.MasterGrouping
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
