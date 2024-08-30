package paxos2bro

import (
	"encoding/json"
	"flag"
	"strconv"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var ephemeralLeader2bro = flag.Bool("ephemeral_leader2", false, "unstable leader, if true paxos replica try to become leader instead of forward requests to current leader")
var read2bro = flag.String("read2", "", "read from \"leader\", \"RFL\", \"quorum\" or \"any\" replica")
var slidewindow = flag.Int("slidewindow length", 5, "length of log that can be committed or executed out of order ")
var highload = flag.Bool("highload", false, "phase 2 broadcast or direct to 1.3 ")

const (
	HTTPHeaderNodeID  = "ID"
	HTTPHeaderSlot    = "Slot"
	HTTPHeaderKeySlot = "KeySlot"
	//HTTPHeaderExecuteSlot = "ExecuteSlot"
	HTTPHeaderkeyStatus  = "KeyStatus"
	HTTPHeaderBallot     = "Ballot"
	HTTPHeaderExecute    = "Execute"
	HTTPHeaderInProgress = "Inprogress"
	HTTPHeaderHole       = "Hole"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Paxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2a)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	// r.Register(paxi.AntiEntropy{}, r.HandleAntiEntropy)
	// r.Register(Pullrequest{}, r.HandlePull)
	// r.Register(Pushrequest{}, r.HandlePush)
	return r
}

//handle requests from client
func (r *Replica) handleRequest(m paxi.Request) {
	//log.Debugf("Replica %s received request\n", r.ID(), m, m.Command.IsRead(), *read2bro)
	if m.Command.IsRead() && *read2bro != "" {
		log.Debugf("Replica %s received read request %v\n", r.ID(), m)
		v, readslot, status, inProgress := r.readInProgress(m)
		reply := paxi.Reply{
			Command:    m.Command,
			Value:      v,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		reply.Properties[HTTPHeaderNodeID] = string(r.ID())
		reply.Properties[HTTPHeaderSlot] = strconv.Itoa(r.Paxos.slot)
		reply.Properties[HTTPHeaderKeySlot] = strconv.Itoa(readslot)
		//reply.Properties[HTTPHeaderExecuteSlot] = strconv.Itoa(r.ExecuteSlot)
		reply.Properties[HTTPHeaderkeyStatus] = status
		reply.Properties[HTTPHeaderBallot] = r.Paxos.ballot.String()
		reply.Properties[HTTPHeaderExecute] = strconv.Itoa(r.Paxos.execute - 1)
		reply.Properties[HTTPHeaderInProgress] = strconv.FormatBool(inProgress)
		logholeJson, err := json.Marshal(r.loghole)
		if err != nil {
			return
		}
		reply.Properties[HTTPHeaderHole] = string(logholeJson)
		m.Reply(reply)
		return
	}

	if *ephemeralLeader2bro || r.Paxos.IsLeader() {
		r.Paxos.HandleRequest(m)
	} else {
		go r.Forward("1.1", m)
	}
}

func (r *Replica) readInProgress(m paxi.Request) (paxi.Value, int, string, bool) {
	// TODO
	// (1) last slot is read?
	// (2) entry in log over writen
	// (3) value is not overwriten command

	// is in progress
	for i := r.Paxos.slot; i >= r.Paxos.execute; i-- {
		if entry, ok := r.Paxos.log.Load(i); ok {
			e := entry.(*Entry)
			if e.Command.Key == m.Command.Key {
				return e.Command.Value, i, string(e.Status), true
			}
		}
	}
	// // not in progress key
	// for i := r.Paxos.execute; i >= 0; i-- {
	// 	if entry, ok := r.Paxos.log.Load(i); ok {
	// 		e := entry.(*Entry)
	// 		if e.Command.Key == m.Command.Key {
	// 			return e.Command.Value, i, string(e.Status), false
	// 		}
	// 	}
	// }
	// not in log
	return r.Node.Execute(m.Command), -1, "", false
}
