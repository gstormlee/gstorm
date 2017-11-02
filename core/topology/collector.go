package topology

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/gstormlee/gstorm/core/tuple"

	"github.com/gstormlee/gstorm/core/topology/group"
)

// TupleData struct
type TupleData struct {
	LastID string
	Result bool
}

// NewTupleData func
func NewTupleData(last string, result bool) *TupleData {
	t := &TupleData{}
	t.LastID = last
	t.Result = result
	return t
}

// Collector struct
type Collector struct {
	TupleDatas map[string]*TupleData
	Grouping   group.IGrouping
	Addr       string
}

// NewCollector func
func NewCollector() *Collector {
	c := &Collector{}
	c.TupleDatas = make(map[string]*TupleData)
	return c
}

// SetGrouping func
func (c *Collector) SetGrouping(g group.IGrouping) {
	c.Grouping = g
}

// SetResult func
func (c *Collector) SetResult(id string, result bool) {
	v, ok := c.TupleDatas[id]
	if ok {
		v.Result = result
		c.TupleDatas[id] = v
	} else {
		t := NewTupleData("", result)
		c.TupleDatas[id] = t
	}
}

// SetLast func
func (c *Collector) SetLast(id, last string) {
	v, ok := c.TupleDatas[id]
	if ok {
		v.LastID = last
		c.TupleDatas[id] = v
	} else {
		t := NewTupleData(last, false)
		c.TupleDatas[id] = t
	}
}

// SetAddr func
func (c *Collector) SetAddr(addr string) {
	c.Addr = addr
}

// Acker func
func (c *Collector) Acker(data tuple.IID) {

	var acker tuple.IID
	if data1, ok := data.(tuple.IData); ok {

		if v, ok := c.TupleDatas[data.GetID()]; ok {
			if v.LastID == "" {
				acker = NewAckerBegin(data.GetID(), c.Addr, data1.GetCurrentID())
			} else {
				if !v.Result {
					i, err := c.Exclusive(data1.GetCurrentID(), v.LastID)
					if err == nil {
						acker = NewAcker(data.GetID(), i)
					}
				} else {
					acker = NewAckerResult(data.GetID(), Failed)
				}
			}
		}
	}
	delete(c.TupleDatas, data.GetID())
	fmt.Println("collector acker, tuple", acker, reflect.TypeOf(acker), reflect.TypeOf(data))

	c.Grouping.Tuple(acker)
}

// Exclusive func
func (c *Collector) Exclusive(current, last string) (int64, error) {
	i, err := strconv.ParseInt(current, 10, 64)
	if err != nil {
		return 0, nil
	}
	i1, err1 := strconv.ParseInt(last, 10, 64)
	if err1 != nil {
		return 0, nil
	}
	return i ^ i1, nil
}
