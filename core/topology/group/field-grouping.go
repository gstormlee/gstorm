package group

import (
	"reflect"

	"github.com/gstormlee/gstorm/core/tuple"
)

type FiledGrouping struct {
	Grouping
	Field string
}

func NewFieldGrouping(field string) *FiledGrouping {
	out := &FiledGrouping{}
	g := NewGrouping()
	out.Grouping = *g
	out.Field = field
	return out
}

// Prepare func
func (g *FiledGrouping) Prepare(out []chan tuple.IID) {
	g.Grouping.OutChan = out
}

// Run func
func (g *FiledGrouping) Run() {
	for {
		data := <-g.inChan
		name := g.Field
		v1 := reflect.ValueOf(data).Elem()
		v := v1.FieldByName(name)
		str := v.String()
		sum := 0
		for _, c := range str {
			sum += int(c)
		}

		idx := sum % len(g.OutChan)

		g.OutChan[idx] <- data
	}
}

// Launch func
func (g *FiledGrouping) Launch() {
	go g.Run()
}

// Tuple func
func (g *FiledGrouping) Tuple(data tuple.IID) {
	g.Grouping.inChan <- data
}
