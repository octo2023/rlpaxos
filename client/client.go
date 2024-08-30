package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	paxos2bro "github.com/ailidani/paxi/rlpaxos"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")
var path = flag.String("historypath", "/bin/history", "client operation history file paht.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	paxi.Client
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := paxi.Key(k)
	v, err := d.Get(key)
	if len(v) == 0 {
		return 0, nil
	}
	x, _ := binary.Uvarint(v)
	return int(x), err
}

func (d *db) Write(k, v int) error {
	key := paxi.Key(k)
	value := make([]byte, 10)
	binary.PutUvarint(value, uint64(v))
	err := d.Put(key, value)
	return err
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}
	log.Debugf("d.Client = paxos.NewClient(paxi.ID(*id)) algorithm %s,%v\n", *algorithm, *algorithm)
	d := new(db)
	switch *algorithm {

	case "paxos2bro":
		d.Client = paxos2bro.NewClient(paxi.ID(*id))
	default:
		d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	}

	b := paxi.NewBenchmark(d)
	if *load {
		b.Load()
	} else {
		b.Run(*path)
	}
}
