package transfer

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"sync"

	"github.com/gstormlee/gstorm/core/etcd"
	"github.com/gstormlee/gstorm/core/utils"
	"github.com/smallnest/rpcx"
)

// Server struct
type Server struct {
	ReadDirectory string
	Server        *rpcx.Server
	Session       *Session
	TopologyDir   string
}

// NewServer func
func NewServer() *Server {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}
	return &Server{ReadDirectory: path.Join(user.HomeDir, "distribute"), Server: rpcx.NewServer()}
}

// ListenAndServe func
func (srv *Server) ListenAndServe(etcdaddr string) {
	session := &Session{mu: &sync.Mutex{}, files: make(map[SessionId]*os.File)}
	fmt.Println(srv.Server)

	srv.Server.RegisterName("FileSession", &FileSession{server: srv, session: session})
	ip, err := utils.GetLocalIP()
	if err == nil {
		addr := ip + ":8972"
		etcd := etcd.NewClient(etcdaddr)

		etcd.Set("/nimbus/addr", addr)
		err1 := srv.Server.Serve("tcp", addr) //"127.0.0.1:8972")
		if err1 != nil {
			fmt.Println(err1)
		}
	}
}
