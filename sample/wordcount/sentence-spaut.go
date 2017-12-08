package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/gstormlee/gstorm/core/topology"
)

// Sentencespout struct
type Sentencespout struct {
	topology.Spout
	//topology.Handle
}

// NewSentencespout func
func NewSentenceSpout(name, node string) *Sentencespout {
	s := &Sentencespout{}
	spout := topology.NewSpout(name, node)
	s.Spout = *spout
	return s
}

// Open func
func (s *Sentencespout) Open(files map[string]string) {
	for _, value := range files {
		f, err := os.Open(value)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		fmt.Println("begin")
		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
			if err == nil && len(line) != 0 {
				l := &SentenceValue{}
				fmt.Println(line)
				l.Sentence = line
				fmt.Printf("addr is %s, spout = %v\n", s.Addr, s)
				l.Addr = s.Addr
				s.Inchan <- l
			}
			if err != nil || io.EOF == err {
				break
			}
		}
	}
}
