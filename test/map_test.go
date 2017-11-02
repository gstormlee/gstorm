package test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	var m sync.Map
	m.Store(1, 2)
	m.Store(2, 3)
	m.Store(3, 4)
	c := time.Tick(1 * time.Second)
	for now := range c {
		m.Range(func(key, value interface{}) bool {
			fmt.Println(key, value, now)
			return true
		})
	}

}
