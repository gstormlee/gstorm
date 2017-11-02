package etcd

import (
	"errors"
	"strconv"
	"strings"
)

// Config struct
type Config struct {
	address string
	port    int
	timeout int
}

// NewConfig func
func NewConfig(flagValue string) (*Config, error) {
	// This parameter is mandatory
	if len(flagValue) <= 0 {
		return nil, errors.New("Node parameter is missing!")
	}

	// Allow 'address:port' format
	items := strings.Split(flagValue, ":")
	if len(items) > 2 {
		return nil, errors.New("Invalid node format!")
	}

	// Basic configuration
	conf := new(Config)
	conf.address = items[0]
	conf.port = 4001

	// If a port is specified using ':port'
	if len(items) > 1 {
		port, err := strconv.Atoi(items[1])
		if err != nil {
			return nil, errors.New("Invalid node port format!")
		}
		conf.port = port
	}
	return conf, nil
}
