package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/gstormlee/gstorm/core/topology"
	"github.com/gstormlee/gstorm/core/tuple"
)

// WordCountBolt struct
type WordCountBolt struct {
	topology.Bolt
	words map[string]int
	lock  sync.Mutex
}

// Prepare func
func (w *WordCountBolt) Prepare() {
	w.words = make(map[string]int)
}

// Execute func
func (w *WordCountBolt) Execute(data tuple.IID) {
	d, ok := data.(*WordValue)
	if ok {
		_, ok := w.words[d.Word]
		if !ok {
			w.words[d.Word] = 0
		}
		w.words[d.Word]++
		w.TupleCollector.SetLast(d.GetID(), "0")
		w.TupleCollector.Acker(d)
		go w.Save()
	}
}

// NewWordCountBolt name machine name
func NewWordCountBolt(name, node string) *WordCountBolt {
	b := &WordCountBolt{}

	bolt := topology.NewBolt(name, node)

	b.Bolt = *bolt
	return b
}

// Run func
func (w *WordCountBolt) Run() {
	for {
		data := <-w.Inchan
		w.Execute(data)
	}
}

// Save func
func (w *WordCountBolt) Save() {

	w.lock.Lock()
	defer w.lock.Unlock()
	name := w.GetName()
	name += ".txt"

	dir, _ := user.Current()
	name = filepath.Join(dir.HomeDir, name)
	fmt.Println(name)
	file, err := os.Create(name)
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	str := ""
	for k, v := range w.words {
		str += k + ":" + strconv.Itoa(v) + "\n"
	}
	_, err1 := file.WriteString(str)
	if err1 != nil {
		fmt.Println(err1)
	}
}
