package topology

import (
	"github.com/gstormlee/gstorm/core/tuple"
)

// AckerReciever struct
type AckerReciever struct {
	//Addr   string
	Server *AckerServer
	//Inchan chan tuple.IID
}

// NewAckerReciever func
func NewAckerReciever(addr string) *AckerReciever {
	reciever := new(AckerReciever)
	reciever.Server = NewAckerServer(addr)
	return reciever
}

// ListenAndServe func
func (r *AckerReciever) ListenAndServe(inchan chan tuple.IID) {
	r.Server.ListenAndServe(inchan)
}
