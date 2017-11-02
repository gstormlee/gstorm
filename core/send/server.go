package send

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/tuple"
	"github.com/smallnest/rpcx"
)

// Server struct
type Server struct {
	Addr      string
	RpcServer *rpcx.Server
	Out       chan tuple.IID
}

type IServer interface {
	Register()
	ListenAndServe(out chan tuple.IID)
}

// NewServer func
func NewServer(addr string) *Server {
	return &Server{Addr: addr, RpcServer: rpcx.NewServer()}
}

// Register func
func (srv *Server) Register() {
	srv.RpcServer.RegisterName("Queue", &Queue{outchan: srv.Out})
}

// ListenAndServe func
func (srv *Server) ListenAndServe(out chan tuple.IID) {
	srv.Out = out
	srv.Register()
	err := srv.RpcServer.Serve("tcp", srv.Addr)
	if err != nil {
		fmt.Println(err)
	}
}
