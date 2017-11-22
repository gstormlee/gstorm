package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/gstormlee/gstorm/transfer"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// Topology struct
type Topology struct {
	Name      string
	groupings map[string]string
	Handle    string
	From      int
	To        int
	Next      string
}

// NewTopology func
func NewTopology() *Topology {
	return &Topology{groupings: make(map[string]string)}
}

// Watch func
func (t *Topology) Watch() {
	fmt.Println("watch topology name")
	data := GetInstance()
	rch := data.EtcdClient.Etcd.Watch(context.Background(), "/topologyname/", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			a := strings.Split(string(ev.Kv.Key[:]), "/")
			key := a[len(a)-1]
			switch ev.Type {
			case mvccpb.PUT:
				fmt.Println("start worker", key)
				go StartWorker(key)
			case mvccpb.DELETE:
			}
		}

	}
}

// StartWorker func
func StartWorker(name string) {
	fmt.Println("StartWorker")
	key := "/topology/" + name + "/file"
	super := GetInstance()
	fmt.Println("etcd addr :", super.EtcdAddr)
	vals, err := super.EtcdClient.Get(key)
	if err == nil && len(vals) > 0 {
		addrs, err1 := super.EtcdClient.Get("/nimbus/addr")

		if err1 == nil {
			addr := addrs[0]
			fmt.Println("distribute", addr)
			client := transfer.NewClient(addr)
			if err := client.Dial(); err != nil {
				panic(err)
			}
			file := filepath.Base(vals[0])
			fmt.Println(vals[0], super.SavePath)
			file = filepath.Join(super.SavePath, file)

			err1 := os.MkdirAll(super.SavePath, 0777)
			if err1 != nil {
				fmt.Println(err1)
				return
			}
			fmt.Println(vals[0], file)
			client.Download(vals[0], file)

			fmt.Println("start command storm", file)
			err = syscall.Chmod(file, 0777)
			if err != nil {
				fmt.Println("chmod error:", err)
				return
			}

			fmt.Println("etcd addr ", super.EtcdAddr)
			command := file + " start --etcd " + super.EtcdAddr + " --topology " + name + " --name " + super.Name
			args := strings.Split(command, " ")
			fmt.Println("start command", command, args)
			cmd := exec.Command(args[0], args[1:]...) //.CombinedOutput()
			stdout, err := cmd.StdoutPipe()

			if err != nil {
				fmt.Println(err)
				return
			}

			cmd.Start()

			reader := bufio.NewReader(stdout)

			//实时循环读取输出流中的一行内容
			for {
				line, err2 := reader.ReadString('\n')
				if err2 != nil || io.EOF == err2 {
					break
				}
				fmt.Println(line)
			}

			cmd.Wait()
		}
	}
}
