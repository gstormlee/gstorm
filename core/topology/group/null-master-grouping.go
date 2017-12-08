package group

import (
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
	return mg
}

// GroupingMessage func
func (mg *NullMasterGrouping) GroupingMessage(msg tuple.IID) {
	for _, v := range mg.Groupings {
		v.Tuple(msg)
	}
}
