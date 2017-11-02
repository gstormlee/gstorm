package data

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

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

func GetSingletonPort() *Port {
	portonce.Do(func() {
		instance = &Port{}
		instance.locks = make(map[string]*sync.Mutex)
	})
	return instance
}
func (p *Port) GetLocalPort(name string, client *etcd.Client) (int, error) {
	key := name + "/" + "port"

	_, ok := p.locks[name]
	if !ok {
		p.locks[name] = new(sync.Mutex)
	}
	p.locks[name].Lock()
	defer p.locks[name].Unlock()

	values, err := client.Get(key)
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	if len(values) == 0 {
		client.Set(key, "10000")
		return 10000, nil
	}
	//svalue := values[0]
	a, err := strconv.Atoi(values[0])
	a++
	str := strconv.FormatInt(int64(a), 10)
	client.Set(key, str)
	return a, nil
}
