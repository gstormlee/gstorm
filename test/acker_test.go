package test

/*
func TestAcker(t *testing.T) {
	t.Skip()
	reciever := topology.NewAckerReciever("127.0.0.1:8091")
	//server := topology.NewAckerServer("127.0.0.1:8091")
	inchan := make(chan tuple.IID, 10)
	//fmt.Println(inchan, reciever)
	go reciever.Server.ListenAndServe(inchan)
	time.Sleep(10 * time.Second)
	sender := topology.NewAckerSender("127.0.0.1:8091")
	sender.Prepare()
	//sender.Client.Connect()
	data := topology.NewAckerBegin("1", "1.1.1.1:100001", "2")
	sender.Send(data)
	d := topology.NewAcker("1", 10)
	sender.Send(d)
}
*/
/*
func TestRpc(t *testing.T) {
	server := rpcx.NewServer()
	out := make(chan tuple.IID, 10)
	server.RegisterName("AckerOp", topology.NewAckerOp(out))
	fmt.Println(server)
	go server.Serve("tcp", "192.168.1.10:8097")
	time.Sleep(10 * time.Second)
	data := topology.NewAckerBegin("1", "1.1.1.1:100001", "2")
	//	sender := topology.NewAckerSender("192.169.1.10:8091")
	//	sender.Prepare()
	//	sender.Send(data)
	s := &rpcx.DirectClientSelector{Network: "tcp", Address: "192.168.1.10:8097", DialTimeout: 10 * time.Second}
	client := rpcx.NewClient(s)
	var r send.Replay
	client.Call(context.Background(), "AckerOp.Begin", data, &r)
}*/
