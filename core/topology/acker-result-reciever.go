package topology

import (
	"sync"
	"github.com/gstormlee/gstorm/core/tuple"
)

// AckerResultReciever struct
type AckerResultReciever struct {
	Addr   string
	Server *AckerServer
	inchan chan tuple.IID
	Datas  sync.Map
	//Datas  map[string]tuple.IID

	Queue chan tuple.IID
}

// NewAckerResultReciever  func
func NewAckerResultReciever(addr string) *AckerResultReciever {
	reciever := new(AckerResultReciever)
	reciever.Addr = addr
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
			if v, ok1 := r.Datas.Load(data.GetID()); ok1 {
				switch acker.Result {
				case Succeeded:
					fmt.Println("消息处理完成", data.GetID())
					r.Datas.Delete(data.GetID())
					break
				case Failed:
					fmt.Println(v)
					//r.Queue <- v
					break
				}
			}
		}
	}
}

func (r *AckerResultReciever) GetInChan() chan tuple.IID {
	return r.inchan
}

// AckerMessage func
func (r *AckerResultReciever) AckerMessage(data tuple.IID) {

	r.Datas.Store(data.GetID(), data)
}

// SetQueue func
func (r *AckerResultReciever) SetQueue(chans chan tuple.IID) {
	r.Queue = chans
}
