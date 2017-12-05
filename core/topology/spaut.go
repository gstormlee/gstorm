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
	Launch()
	SetAckerReciever(reciever *AckerResultReciever)
	AddPending(data tuple.IID)
	GetQueue() chan tuple.IID
}

// Spout struct
type Spout struct {
	Handle
	AckerResultHandle
	Acker *AckerResultReciever
	//Pendings sync.Map //[string]tuple.IID
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
	s.Emmitter(data)
	s.AddPending(data)
	s.TupleCollector.Acker(data)
}

// AddPending func
func (s *Spout) AddPending(data tuple.IID) {
	//d := NewPendingData(data)
	//s.Pendings.Store(data.GetID(), d)
}

// GetInchan func
func (s *Spout) GetInchan() chan tuple.IID {
	return s.Queue
}

// GetQueue func
func (s *Spout) GetQueue() chan tuple.IID {
	return s.Inchan
}

// NewSpout func
func NewSpout(server, node string) *Spout {
	s := &Spout{}
	h := NewHandle(node)
	s.Handle = *h
	//s.Queue = make(chan tuple.IID, 10)
	s.timer = time.NewTimer(time.Second)
	//go s.OnTimer()
	ah := &AckerResultHandle{}
	ah.Queue = make(chan tuple.IID, 10)
	s.AckerResultHandle = *ah
	return s
}

// AckerResultHandle struct
type AckerResultHandle struct {
	Queue chan tuple.IID
}

// Loop func
func (a *AckerResultHandle) Loop() {
	for {
		acker := <-a.Queue
		a.Execute(acker)
	}
}

// Execute func
func (a *AckerResultHandle) Execute(data tuple.IID) {
}

// Launch func
func (a *AckerResultHandle) Launch() {
	go a.Loop()
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
