package topology

import (
	"fmt"
	"strconv"

	"github.com/gstormlee/gstorm/core/topology/group"
	"github.com/gstormlee/gstorm/core/tuple"
)

// TupleData struct
type TupleData struct {
	LastID []string
	Result bool
}

// NewTupleData func
func NewTupleData(last string, result bool) *TupleData {
	t := &TupleData{}
	t.LastID = append(t.LastID, last)
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
	}
}

// SetLast func
func (c *Collector) SetLast(id, last string) {
	v, ok := c.TupleDatas[id]
	if ok {
		fmt.Printf("before set lastid =%s, %v, last = %s\n", id, v, last)
		has := false
		for _, id := range v.LastID {
			if id == last {
				has = true
				break
			}
		}

		if !has {
			v.LastID = append(v.LastID, last)
		}
		c.TupleDatas[id] = v
	} else {
		d := NewTupleData(last, false)
		c.TupleDatas[id] = d
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
			if len(v.LastID) == 0 {
				acker = NewAckerBegin(data.GetID(), c.Addr, data1.GetCurrentID())
			} else {
				if !v.Result {
					d, err := strconv.ParseInt(data1.GetCurrentID(), 10, 64)
					fmt.Printf("data= %v, v = %v, current = %d\n", data, v, d)
					if err != nil {
						fmt.Println(err)
					}
					e := false
					for _, id := range v.LastID {
						if d, err = c.Exclusive(d, id); err != nil {
							fmt.Println(err)
							e = true
							break
						}
					}

					if !e {
						acker = NewAcker(data.GetID(), d)
					}

				} else {
					acker = NewAckerResult(data.GetID(), Failed)
				}
			}

		} else {
			acker = NewAckerBegin(data.GetID(), c.Addr, data1.GetCurrentID())
			fmt.Printf("collector id = [%s], ackerbegin %v, data= %v\n", data.GetID(), acker, data)
		}
	}
	delete(c.TupleDatas, data.GetID())
	fmt.Printf("collector after delete %v, %v\n", c.TupleDatas, acker)
	c.Grouping.Tuple(acker)
}

// Exclusive func
func (c *Collector) Exclusive(current int64, last string) (int64, error) {
	i1, err1 := strconv.ParseInt(last, 10, 64)
	if err1 != nil {
		return 0, nil
	}
	return current ^ i1, nil
}
