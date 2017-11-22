package test

import (
	"testing"
)

func TestAckerResult(t *testing.T) {
	t.Skip()
	/*reciever := topology.NewAckerResultReciever("127.0.0.1:9001")
	inchan := make(chan tuple.IID, 10)
	go reciever.Server.ListenAndServe(inchan)

	sender := topology.NewAckerSender("127.0.0.1:9001")
	sender.Prepare()
	//go sender.Run()
	data := topology.NewAckerResult("1", 10)
	sender.Send(data)*/
}
