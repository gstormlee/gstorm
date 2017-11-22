package group

import "github.com/gstormlee/gstorm/core/tuple"

// AllMasterGrouping struct
type AllMasterGrouping struct {
	MasterGrouping
}

// NewAllMasterGrouping func
func NewAllMasterGrouping() *AllMasterGrouping {
	mg := &AllMasterGrouping{}
	mg.MasterGrouping = *NewMasterGrouping()
	mg.MasterGrouping.Sub = mg
	return mg
}

// GroupingMessage func
func (mg *AllMasterGrouping) GroupingMessage(msg tuple.IID) {
	for _, v := range mg.Groupings {
		v.Tuple(msg)
	}
}
