package group

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/tuple"
)

// IMasterGrouping interface
type IMasterGrouping interface {
	Run()
	Lanuch()
	Tuple(t tuple.IID)
	GroupingMessage(msg tuple.IID)
	AddGrouping(next string, g IGrouping)
	GetGroupingMap() map[string]IGrouping
	GetGrouping(name string) IGrouping
	GetChan() chan tuple.IID
	CreateRunner() IRunner
}

// MasterGrouping struct
type MasterGrouping struct {
	InChan    chan tuple.IID
	Groupings map[string]IGrouping
}

// NewMasterGrouping func
func NewMasterGrouping() *MasterGrouping {
	mg := &MasterGrouping{}
	mg.Groupings = make(map[string]IGrouping)
	mg.InChan = make(chan tuple.IID, 10)
	return mg
}

// Run func
func (mg MasterGrouping) Run() {

}

// GroupingMessage func
func (mg *MasterGrouping) GroupingMessage(msg tuple.IID) {
	fmt.Println("grouping message ************")
}

// Tuple func
func (mg *MasterGrouping) Tuple(msg tuple.IID) {
	mg.InChan <- msg
}

func (mg *MasterGrouping) Lanuch() {
	go mg.Run()
}

// AddGrouping func
func (mg *MasterGrouping) AddGrouping(next string, g IGrouping) {
	mg.Groupings[next] = g
}

func (mg *MasterGrouping) GetGroupingMap() map[string]IGrouping {
	return mg.Groupings
}

func (mg *MasterGrouping) GetGrouping(name string) IGrouping {
	if v, ok := mg.Groupings[name]; ok {
		return v
	}
	return nil
}

func (mg *MasterGrouping) CreateRunner() IRunner {
	return NewMasterGroupingRunner()
}
func (mg *MasterGrouping) GetChan() chan tuple.IID {
	return mg.InChan
}
type MasterGroupingRunner struct {
}
func NewMasterGroupingRunner() IRunner {
	return &MasterGroupingRunner{}
}

func (mgr *MasterGroupingRunner) Run(mg interface{}) {
	for {
		fmt.Println("master grouping run")
		if g, ok := mg.(IMasterGrouping); ok {
			msg := <-g.GetChan()
			g.GroupingMessage(msg)
		}
	}
}
