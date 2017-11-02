package test

import (
	"fmt"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	go Ontimer()
	time.Sleep(time.Second * 30)
}

func Ontimer() {
	t1 := time.NewTimer(time.Second * 1)
	for {
		select {
		case <-t1.C:
			fmt.Println("on timer")
		}
	}
}
