package group

import (
	"fmt"
	"reflect"

	"github.com/gstormlee/gstorm/core/tuple"
)

// NullMasterGrouping struct
type NullMasterGrouping struct {
	MasterGrouping
}

// NewNullMasterGrouping func
func NewNullMasterGrouping() *NullMasterGrouping {
	mg := &NullMasterGrouping{}
	mg.MasterGrouping = *NewMasterGrouping()
	mg.MasterGrouping.Sub = mg
	return mg
}

// GroupingMessage func
func (mg *NullMasterGrouping) GroupingMessage(msg tuple.IID) {
	fmt.Println("nullmastergrouping", len(mg.Groupings), mg.Groupings)

	for _, v := range mg.Groupings {
		fmt.Println("range ")
		fmt.Println("---------------nullmaster grouping", msg, "_____________", reflect.TypeOf(v))
		v.Tuple(msg)
	}
}
