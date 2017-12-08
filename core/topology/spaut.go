package topology

import (
	"time"

	"github.com/gstormlee/gstorm/core/tuple"
)

// ISpout interface
type ISpout interface {
	Open(conf map[string]string)
	NextTuple(data tuple.IID)
	Run()
	SetAckerReciever(reciever *AckerResultReciever)
}

// Spout struct
type Spout struct {
	Handle
	Acker *AckerResultReciever
	timer *time.Timer
}

// Open func
func (s *Spout) Open(conf map[string]string) {
}

// Run func
func (s *Spout) Run() {
	for {
		m := <-s.Inchan
		s.NextTuple(m)
	}
}

// PendingData struct
type PendingData struct {
	Data   tuple.IID
	Happen int64
}

// NewPendingData func
func NewPendingData(data tuple.IID) *PendingData {
	d := &PendingData{}
	d.Happen = time.Now().Unix()
	return d
}

// NextTuple func
func (s *Spout) NextTuple(data tuple.IID) {
	g := GetGlobalGenerator()
	data.SetID(g.GenerateID())
	s.Acker.AckerMessage(data)
	s.Emmitter(data)
	s.TupleCollector.Acker(data)
}

// NewSpout func
func NewSpout(server, node string) *Spout {
	s := &Spout{}
	h := NewHandle(node)
	s.Handle = *h
	s.timer = time.NewTimer(time.Second)

	return s
}

// SetAckerReciever func
func (s *Spout) SetAckerReciever(r *AckerResultReciever) {
	s.Acker = r
}
// OnTimer func
// func (s *Spout) OnTimer() {
// 	c := time.Tick(1 * time.Second)
// 	for now := range c {
// 		s.Pendings.Range(func(key, value interface{}) bool {
// 			if d, ok := value.(PendingData); ok {
// 				duration := now.Unix() - d.Happen
// 				fmt.Println(duration, now.Unix)
// 				if duration >= 3 {
// 					s.Pendings.Delete(key)
// 				}
// 			}
// 			return true
// 		})
// 	}
// }
