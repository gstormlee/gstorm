package send

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gstormlee/gstorm/core/tuple"
)

// Queue struct
type Queue struct {
	outchan   chan tuple.IID
	Factories MessageFactories
}

// Message struct
type Message struct {
	DataType string
	Data     string
}

// IMessageFactory interface
type IMessageFactory interface {
	Create(data Message) tuple.IID
}

// MessageFactories struce
type MessageFactories struct {
	Factories []IMessageFactory
}

// Register func
func (f *MessageFactories) Register(f1 IMessageFactory) {
	f.Factories = append(f.Factories, f1)
}

// NewQueue func
func NewQueue(outchan chan tuple.IID, factory IMessageFactory) *Queue {
	queue := &Queue{}
	queue.outchan = outchan
	queue.Factories.Register(factory)
	return queue
}

// PushData func
func (q *Queue) PushData(data Message, r *Replay) error {
	fmt.Printf("******((((((((((((recieve data )))))))))))%v\n", data)
	var message tuple.IID
	for _, f := range q.Factories.Factories {
		message = f.Create(data)
		if message != nil {

			json.Unmarshal([]byte(data.Data), &message)
			fmt.Printf("recieve data %v, type = %v\n", message, reflect.TypeOf(message))
			q.outchan <- message
			break
		}
	}
	return nil
}

// func (q *Queue) Push(data tuple.SentenceValue, r *Replay) error {
// 	q.outchan <- &data
// 	r.Answer = "00"
// 	return nil
// }

// func (q *Queue) PushNsq(data tuple.NsqType, r *Replay) error {
// 	q.outchan <- &data
// 	r.Answer = "00"
// 	return nil
// }

// func (q *Queue) PushWord(data tuple.WordValue, r *Replay) error {
// 	q.outchan <- &data
// 	r.Answer = "00"
// 	return nil
// }

// Start func
func Start(topology string) {

}

// Replay struct
type Replay struct {
	Answer string
}
