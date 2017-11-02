package main

import (
	"bufio"
	"io"
	"os"

	"github.com/gstormlee/gstorm/core/topology"

	"github.com/gstormlee/gstorm/core/tuple"
)

// Sentencespout struct
type Sentencespout struct {
	topology.Spout
	//topology.Handle
}

// NewSentencespout func
func NewSentenceSpout(name, node string) *Sentencespout {
	//Sentencespout{Query: make(chan IID, 10), Base: 0, Name: "Sentencespout"}
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

		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
			if err == nil && len(line) != 0 {
				l := &tuple.SentenceValue{}
				l.Sentence = line
				s.Inchan <- l
			}
			if err != nil || io.EOF == err {
				break
			}
		}
	}
}
