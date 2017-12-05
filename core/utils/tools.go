package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/gstormlee/gstorm/core/etcd"
)

func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
		return "", err
	}

	for _, address := range addrs {

		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("ip no foud !")

}

var instance *Port
var portonce sync.Once

type Port struct {
	locks map[string]*sync.Mutex
}

// GetSingletonPort func
func GetSingletonPort() *Port {
	portonce.Do(func() {
		instance = &Port{}
		instance.locks = make(map[string]*sync.Mutex)
	})
	return instance
}

// GetLocalPort func
func (p *Port) GetLocalPort(name string, client *etcd.Client) (int, error) {
	s1, err := concurrency.NewSession(client.Etcd)

	defer s1.Close()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	m1 := concurrency.NewMutex(s1, "/"+name+"-lock/")
	if err1 := m1.Lock(context.TODO()); err1 != nil {
		fmt.Println(err1)
		return 0, err1
	}
	defer m1.Unlock(context.TODO())
	key := name + "/" + "port"
	vals, err2 := client.Get(key)
	if err2 != nil {
		fmt.Println(err2)
		return 0, err2
	}

	if len(vals) == 1 {
		serial, err3 := strconv.Atoi(vals[0])
		if err3 != nil {
			return 0, err3
		}
		serial++
		str := strconv.Itoa(serial)
		client.Set(key, str)
		return serial, nil
	} else if len(vals) == 0 {
		client.Set(key, "10001")
		return 10001, nil
	}
	return 0, errors.New("can not found key")
}
