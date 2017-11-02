package etcd

import (
	"time"
)

// Value struct
type Value struct {
	key   string
	value string
	ttl   time.Duration
}

// NewValue func
func NewValue(key string, v string, ttl int) *Value {
	value := new(Value)
	value.key = key
	value.value = v
	if ttl > 0 {
		value.ttl = time.Millisecond * time.Duration(ttl)
	}
	return value
}
