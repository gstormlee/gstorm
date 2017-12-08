package main

import "github.com/nsqio/go-nsq"
import "github.com/gstormlee/gstorm/core/topology"
import "github.com/gstormlee/gstorm/core/tuple"

type NsqSpout struct {
	topology.Spout
	pendinglist []string
	consumer    *nsq.Consumer
}

type Message struct {
	tuple.ID
	msg string
}

//NewNsqSpout return a pointer of new nsqspout instance
func NewNsqSpout(name, node string) *NsqSpout {
	s := &NsqSpout{}
	spout := topology.NewSpout(name, node)
	s.Spout = *spout
	return s
}

//Open func
func (s *NsqSpout) Open(topic, channel, address string) {
	println("in nsq spout")
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("test", "test1", config)
	if err != nil {
		panic(err)
	}
	consumer.AddHandler(s)
	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		panic(err)
	}
	s.consumer = consumer
}

//HandleMessage func
func (s *NsqSpout) HandleMessage(msg *nsq.Message) error {
	//s.pendinglist = append(s.pendinglist, mag)
	data := &Message{}
	data.msg = string(msg.Body)
	println(data)
	s.Inchan <- data
	return nil
}
