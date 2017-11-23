package topology

import (
	"github.com/gstormlee/gstorm/core/tuple"
)

// AckerResultReciever struct
type AckerResultReciever struct {
	Addr   string
	Server *AckerServer
	inchan chan tuple.IID
	Datas  map[string]tuple.IID

	Queue chan tuple.IID
}

// NewReciever func
func NewAckerResultReciever(addr string) *AckerResultReciever {
	reciever := new(AckerResultReciever)
	reciever.Server = NewAckerServer(addr)
	reciever.inchan = make(chan tuple.IID, 10)
	return reciever
}

// ListenAndServe func
func (r *AckerResultReciever) ListenAndServe(inchan chan tuple.IID) {
	r.Server.ListenAndServe(inchan, nil)
}

// Run func
func (r *AckerResultReciever) Run() {
	for {
		data := <-r.inchan
		acker, ok := data.(*AckerResult)
		if ok {
			if v, ok1 := r.Datas[data.GetID()]; ok1 {
				switch acker.Result {
				case Succeeded:
					delete(r.Datas, data.GetID())
					break
				case Failed:
					r.Queue <- v
					break
				}
			}
		}
	}
}

// AckerMessage func
func (r *AckerResultReciever) AckerMessage(data tuple.IID) {
	r.Datas[data.GetID()] = data
}

// SetQueue func
func (r *AckerResultReciever) SetQueue(chans chan tuple.IID) {
	r.Queue = chans
}
