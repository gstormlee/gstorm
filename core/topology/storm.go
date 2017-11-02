package topology

/*
// Worker struct
type Worker struct {
	Name     string
	NodeName string
	Index    int
	Next     string
	Start    bool
	Grouping string
	NType    string
	Field    string
}

// Storm struct
type Storm struct {
	Serial         int
	Name           string
	EtcdAddr       string
	TopologyName   string
	EtcdClient     *etcd.Client
	WatchNodes     map[string][]*Worker
	Ends           []*Worker
	Ackers         []*Worker
	Starts         []ISpout
	StartWorkers   []*Worker
	CreatedWorkers []IHandle
	acker          IHandle
	AckerGrouping  group.IGrouping
}

var storm *Storm
var once sync.Once
var EtcdAddr string

// GetStorm func
func GetStorm() *Storm {
	fmt.Println("etcd addr", EtcdAddr)
	once.Do(func() {
		storm = &Storm{}
		storm.EtcdClient = etcd.NewClient(EtcdAddr)
		storm.WatchNodes = make(map[string][]*Worker)

	})
	return storm
}
*/
