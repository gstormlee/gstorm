package send

import "github.com/gstormlee/gstorm/core/tuple"

// Queue struct
type Queue struct {
	outchan chan tuple.IID
}

func NewQueue(outchan chan tuple.IID) *Queue {
	queue := &Queue{}
	queue.outchan = outchan
	return queue
}

func (q *Queue) Push(data tuple.SentenceValue, r *Replay) error {
	q.outchan <- &data
	r.Answer = "00"
	return nil
}

func (q *Queue) PushNsq(data tuple.NsqType, r *Replay) error {
	q.outchan <- &data
	r.Answer = "00"
	return nil
}

func (q *Queue) PushWord(data tuple.WordValue, r *Replay) error {
	q.outchan <- &data
	r.Answer = "00"
	return nil
}

// Start func
func Start(topology string) {

}

// Replay struct
type Replay struct {
	Answer string
}
