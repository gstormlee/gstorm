package etcd

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type Etcd struct {
	KeysApi client.KeysAPI
}

func NewETCD() *Etcd {
	return &Etcd{}
}

var e *Etcd
var once sync.Once

func GetInstance() *Etcd {
	once.Do(func() {
		e = NewETCD()
	})
	return e
}

func (e *Etcd) Connect() {
	cfg := client.Config{
		Endpoints: []string{"127.0.0.1:2379"},
		//Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	e.KeysApi = client.NewKeysAPI(c)
}

func (e *Etcd) Mkdir(dir string) error {
	o := client.SetOptions{Dir: true}
	_, err := e.KeysApi.Set(context.Background(), dir, "", &o)
	if err != nil {
		fmt.Println("error ", err)
		//log.Fatal(err)
		return err
	}

	return nil
}

func (e *Etcd) Set(key, value string) {
	resp, err := e.KeysApi.Set(context.Background(), key, value, nil)
	if err != nil {
		log.Fatal(err)
	} else {
		// print common key info
		log.Printf("Set is done. Metadata is %q\n", resp)
	}
}

func (e *Etcd) Get(key string) (client.Nodes, error) {
	resp, err := e.KeysApi.Get(context.Background(), key, nil)
	if err != nil {
		log.Fatal(err)
		return nil, err
	} else {
		//log.Printf("Get is done. Metadata is %q\n", resp)
		//log.Printf("%q key has %q value\n", resp.Node.Nodes)
		return resp.Node.Nodes, nil
	}
}
