package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"

	"github.com/gstormlee/gstorm/core/etcd"
	"github.com/gstormlee/gstorm/transfer"
	"github.com/spf13/cobra"
)

var etcdaddr string
var distributeFile string
var stormFile string

// NewDistributeCommand func
func NewDistributeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "distribute ---------",
		Short: "distribute  the storm run file by topology file",
		Long:  "",
		Run:   distributeFunc,
	}

	cmd.Flags().StringVar(&etcdaddr, "etcd", "e", "eted ip and port")
	cmd.Flags().StringVar(&stormFile, "bin", "s", "storm run file")
	cmd.Flags().StringVar(&distributeFile, "topology", "t", "storm topology file")
	return cmd
}

func distributeFunc(cmd *cobra.Command, args []string) {
	etcdClient := etcd.NewClient(etcdaddr)

	addrs, err1 := etcdClient.Get("/nimbus/addr")

	if err1 == nil {
		addr := addrs[0]
		client := transfer.NewClient(addr)
		if err := client.Dial(); err != nil {
			panic(err)
		}
		userInfo, _ := user.Current()
		UploadDir := path.Join(userInfo.HomeDir, "distribute")

		newPath := filepath.Join(UploadDir, stormFile)
		fmt.Println("----dir---------", UploadDir, stormFile, newPath)
		err := os.MkdirAll(newPath, 0777)
		if err == nil {
			path1 := filepath.Join(newPath, stormFile)
			client.Upload(stormFile, path1)
			path := filepath.Join(newPath, distributeFile)
			client.Upload(distributeFile, path)
			client.UploadFinish(path, path1)
		} else {
			fmt.Println("make dir error:", err)
		}
	}
}

func argOrStdin(args []string, stdin io.Reader, i int) (string, error) {
	if i < len(args) {
		return args[i], nil
	}
	bytes, err := ioutil.ReadAll(stdin)
	if string(bytes) == "" || err != nil {
		return "", errors.New("no available argument and stdin")
	}
	return string(bytes), nil
}
