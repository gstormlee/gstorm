package send

import (
	"github.com/gstormlee/gstorm/core/tuple"
)

type IReciever interface {
	ListenAndServe(inchan chan tuple.IID)
}
type Reciever struct {
	Addr   string
	Server *Server
	inchan chan tuple.IID
}

// NewReciever func
func NewReciever(addr string) *Reciever {
	reciever := new(Reciever)
	reciever.Server = NewServer(addr)
	return reciever
}

// ListenAndServe func
func (r *Reciever) ListenAndServe(inchan chan tuple.IID) {
	r.Server.ListenAndServe(inchan)
}
