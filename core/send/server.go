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
	ListenAndServe(out chan tuple.IID, f IMessageFactory)
}

// NewServer func
func NewServer(addr string) *Server {
	return &Server{Addr: addr, RpcServer: rpcx.NewServer()}
}

// Register func
func (srv *Server) Register(f IMessageFactory) {
	queue := NewQueue(srv.Out, f)
	srv.RpcServer.RegisterName("Queue", queue)
}

// ListenAndServe func
func (srv *Server) ListenAndServe(out chan tuple.IID, f IMessageFactory) {
	srv.Out = out
	srv.Register(f)
	err := srv.RpcServer.Serve("tcp", srv.Addr)
	if err != nil {
		fmt.Println(err)
	}
}
