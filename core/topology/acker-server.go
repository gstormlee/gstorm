package topology

import (
	"fmt"

	"github.com/gstormlee/gstorm/core/tuple"
	"github.com/smallnest/rpcx"
)

// AckerServer struct
type AckerServer struct {
	Addr      string
	RpcServer *rpcx.Server
	Out       chan tuple.IID
}

// NewAckerServer func
func NewAckerServer(addr string) *AckerServer {
	return &AckerServer{Addr: addr, RpcServer: rpcx.NewServer()}
}

// Register func
func (srv *AckerServer) Register() {
	srv.RpcServer.RegisterName("AckerOp", NewAckerOp(srv.Out))
}

// ListenAndServe func
func (srv *AckerServer) ListenAndServe(out chan tuple.IID) {
	srv.Out = out
	srv.Register()
	err := srv.RpcServer.Serve("tcp", srv.Addr)
	if err != nil {
		fmt.Println("listen and serve error:", err)
	}
}
