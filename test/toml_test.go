package test

import (
	"fmt"
	"testing"

	"github.com/gstormlee/gstorm/nimbus/distribute"
)

func TestString(t *testing.T) {
	var s string
	fmt.Println("string", s)
	if s == "" {
		fmt.Println("ok")
	}
}
func TestJSON(t *testing.T) {

	fmt.Println("toml")
	fname := "distribute1.json"
	if json, err := distribute.ReadTopology(fname); err == nil {
		fmt.Println(json)
	}
	t.Log("ok")
}
