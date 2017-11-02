package main

import (
	"fmt"
	"os"
	"path"

	"github.com/gstormlee/gstorm/transfer"
)

func download(name string) {
	data := GetInstance()
	//data.EtcdClient = new etcd.Client(data.EtcdAddr)
	addrs, err1 := data.EtcdClient.Get("/nimbus/addr")
	var addr string
	if err1 != nil && len(addrs) == 1 {
		addr = addrs[0]
		client := transfer.NewClient(addr)
		if err := client.Dial(); err != nil {
			panic(err)
		}
		dir := path.Dir(name)
		fmt.Println("dir", dir)
		os.MkdirAll(dir, 0777)
		key := "/topology/" + name + "/file"
		files, err2 := data.EtcdClient.Get(key)
		var file string
		if err2 != nil && len(files) == 1 {
			file = files[0]
		}
		filepath := path.Join(dir, file)
		//hfile = os.Create(file)
		client.DownloadAt(filepath, file, 0)
	}

}
