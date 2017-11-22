package etcd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	etcdTimeout = 5 * time.Second
)

//Client struct
type Client struct {
	Etcd *clientv3.Client
}

// NewClient
func NewClient(addr string) *Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: etcdTimeout,
	})
	if err != nil {
		fmt.Println("error", err)
		return nil
	}
	c := new(Client)
	c.Etcd = cli
	return c
}

// Get func
func (c *Client) Get(key string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	resp, err := c.Etcd.Get(ctx, key)
	cancel()
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	var values []string
	for _, ev := range resp.Kvs {
		values = append(values, string(ev.Value[:]))
	}

	return values, nil
}

// Set func key value
func (c *Client) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	_, err := c.Etcd.Put(ctx, key, value)
	cancel()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// Delete func
func (c *Client) Delete(key string) (bool, error) {

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	// delete the keys
	_, err := c.Etcd.Delete(ctx, key, nil)
	if err != nil {
		log.Fatal(err)
		return false, err
	}
	return true, nil
}

// DeleteWithPreFix
func (c *Client) DeleteWithPreFix(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	defer cancel()

	// delete the keys
	_, err := c.Etcd.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
		return false, nil
	}

	return true, nil
}

// GetSortedPrefix func
func (c *Client) GetSortedPrefix(key string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	resp, err := c.Etcd.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	cancel()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	//var values map[stirng]string
	values := make(map[string]string)

	for _, ev := range resp.Kvs {
		values[string(ev.Key[:])] = string(ev.Value[:])
	}
	return values, nil
}

// Grant func
func (c *Client) Grant(key, value string, t int) error {
	resp, err := c.Etcd.Grant(context.TODO(), int64(t))
	if err != nil {
		log.Fatal(err)
		return err
	}

	// after 5 seconds, the key 'foo' will be removed
	_, err = c.Etcd.Put(context.TODO(), key, value, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// KeepAliveOnce func
func (c *Client) KeepAliveOnce(key, value string, t int) {
	resp, err := c.Etcd.Grant(context.TODO(), int64(t))
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.Etcd.Put(context.TODO(), key, value, clientv3.WithLease(resp.ID))
	if err != nil {
		log.Fatal(err)
	}

	// to renew the lease only once
	_, kaerr := c.Etcd.KeepAliveOnce(context.TODO(), resp.ID)
	if kaerr != nil {
		log.Fatal(kaerr)
	}
}

// Watch func
func (c *Client) WatchKey(key string) {
	rch := c.Etcd.Watch(context.Background(), key, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}
