package topology

import (
	"github.com/gstormlee/gstorm/core/tuple"
)

// Replay struct
type Replay struct {
	Answer string
}

// AckerOp struct
type AckerOp struct {
	Outchan chan tuple.IID
}

// NewAckerOp func
func NewAckerOp(Outchan chan tuple.IID) *AckerOp {
	queue := &AckerOp{}
	queue.Outchan = Outchan
	return queue
}

// Begin func
func (a *AckerOp) Begin(data AckerBegin, r *Replay) error {
	a.Outchan <- &data
	r.Answer = "00"
	return nil
}

// Acker func
func (a *AckerOp) Acker(data Acker, r *Replay) error {
	a.Outchan <- &data
	r.Answer = "00"
	return nil
}

// Finish func
func (a *AckerOp) Finish(data AckerResult, r *Replay) error {
	a.Outchan <- &data
	r.Answer = "00"
	return nil
}
