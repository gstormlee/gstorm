package topology

import "github.com/gstormlee/gstorm/core/tuple"

const (
	Succeeded = 1
	Normal    = 0
	Failed    = -1
)

// IAcker interface
type IAcker interface {
	GetID() string
}

// AckerBegin struct
type AckerBegin struct {
	tuple.ID
	Addr      string
	CurrentID string
}

// Acker struct
type Acker struct {
	tuple.ID
	Result int64
}

type AckerResult struct {
	tuple.ID
	Result int
}

// AckerData struct
type AckerData struct {
	tuple.ID
	Addr   string
	Result int64
}

// NewAckerData func
func NewAckerData(id, addr string) *AckerData {
	acker := &AckerData{}
	id1 := tuple.ID{}
	id1.SetID(id)
	acker.ID = id1
	acker.Addr = addr
	return acker
}

// NewAckerBegin func
func NewAckerBegin(id, addr, current string) *AckerBegin {
	acker := &AckerBegin{}
	id1 := tuple.ID{}
	id1.SetID(id)
	acker.ID = id1
	acker.Addr = addr
	acker.CurrentID = current
	return acker
}

func NewAcker(id string, result int64) *Acker {
	acker := &Acker{}
	id1 := tuple.ID{}
	id1.SetID(id)
	acker.ID = id1
	acker.Result = result
	return acker
}

func NewAckerResult(id string, result int) *AckerResult {
	acker := &AckerResult{}
	id1 := tuple.ID{}
	id1.SetID(id)
	acker.ID = id1
	acker.Result = result
	return acker
}
