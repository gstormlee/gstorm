package main

import (
	"github.com/gstormlee/gstorm/core/send"
	"github.com/gstormlee/gstorm/core/tuple"
)

// MessageFactory struct
type MessageFactory struct {
}

// Create func
func (f *MessageFactory) Create(data send.Message) tuple.IID {
	switch data.DataType {
	case "SentenceValue":
		return &SentenceValue{}
	case "WordValue":
		return &WordValue{}
	default:
		return nil
	}

}

// SentenceValue struce
type SentenceValue struct {
	Sentence string
	tuple.ID
}

type IWordValue interface {
	GetWord() string
}

type WordValue struct {
	Word string
	tuple.ID
}

// GetWord func
func (w *WordValue) GetWord() string {
	return w.Word
}
