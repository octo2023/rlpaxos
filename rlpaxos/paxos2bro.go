package paxos2bro

import (
	"sync"
	"time"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type Status string

const (
	Accept  Status = "accept"
	Commit  Status = "committed"
	Execute Status = "executed"
)

// entry in log
type Entry struct {
	//slot_entry    int
	Ballot        paxi.Ballot
	Command       paxi.Command
	Commutativity bool // a entry that
	Status        Status
	Commit        bool
	Request       *paxi.Request
	Quorum        *paxi.Quorum
	Timestamp     time.Time
}

// Paxos instance
type Paxos struct {
	paxi.Node

	config []paxi.ID

	log      sync.Map // log ordered by slot
	logMutex sync.Mutex

	execute int         // next execute slot number
	active  bool        // active leader
	ballot  paxi.Ballot // highest ballot number
	slot    int         // highest slot number

	ExecuteSlot int         //The highest slot for executing continuous logs
	loghole     map[int]int //0 is hole, 1 is accept, 2 is committed

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(*Paxos)) *Paxos {
	p := &Paxos{
		Node: n,
		log:  sync.Map{},
		slot: -1,

		ExecuteSlot: -1,
		loghole:     make(map[int]int, 0),

		quorum:          paxi.NewQuorum(),
		requests:        make([]*paxi.Request, 0),
		Q1:              func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:              func(q *paxi.Quorum) bool { return q.Majority() },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// IsLeader indecates if this node is current leader
func (p *Paxos) IsLeader() bool {
	return false
	//return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *Paxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
func (p *Paxos) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
func (p *Paxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// HandleRequest handles request and start phase 1 or phase 2
func (p *Paxos) HandleRequest(r paxi.Request) {
	log.Debugf("Replica %s received %v\n", p.ID(), r)
	if !p.active {
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		log.Debugf("Replica received")
		p.P2a(&r)
	}
}

// P1a starts phase 1 prepare
func (p *Paxos) P1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.Broadcast(P1a{Ballot: p.ballot})
}

// leader check the commutativity of a entry
func (p *Paxos) checkcommutativity(slot int, r *paxi.Request) bool {

	startIndex := slot - *slidewindow

	if startIndex < 0 {
		startIndex = 0
	}

	for i := startIndex; i < slot; i++ {
		value, ok := p.log.Load(i)
		if !ok {
			// Entry 不存在
			continue
		}
		// 将值转换为 *Entry 类型
		entry, ok := value.(*Entry)
		if !ok {
			// 类型断言失败
			continue
		}
		// 比较 Command 的 Key
		if entry.Command.Key == r.Command.Key {
			return true
		}
	}

	return false
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *paxi.Request) {
	p.slot++
	commutative := p.checkcommutativity(p.slot, r)
	entry := &Entry{
		//slot_entry:    p.slot,
		Ballot:        p.ballot,
		Command:       r.Command,
		Commutativity: commutative,
		Request:       r,
		Status:        Accept,
		Quorum:        paxi.NewQuorum(),
		Timestamp:     time.Now(),
	}
	p.log.Store(p.slot, entry)
	entry.Quorum.ACK(p.ID())
	m := P2a{
		ID:            p.ID(),
		Ballot:        p.ballot,
		Slot:          p.slot,
		Commutativity: commutative,
		Command:       r.Command,
		Request:       r,
		Status:        Accept,
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
	//writeLogToFile(p.log)
}

// acceptor HandleP1a handles P1a message
func (p *Paxos) HandleP1a(m P1a) {
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// TODO use BackOff time or forward
		// forward pending requests to new leader
		p.forward()
		// if len(p.requests) > 0 {
		// 	defer p.P1a()
		// }
	}

	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {

		if value, ok := p.log.Load(s); ok {
			if entry, ok := value.(*Entry); ok {
				if !entry.Commit {
					l[s] = CommandBallot{entry.Command, entry.Ballot}
				}
			}
		}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if value, ok := p.log.Load(s); ok {
			if entry, ok := value.(*Entry); ok {
				if !entry.Commit && cb.Ballot > entry.Ballot {
					entry.Ballot = cb.Ballot
					entry.Command = cb.Command
				}
			}
		} else {
			newEntry := &Entry{
				Ballot:  cb.Ballot,
				Command: cb.Command,
				Commit:  false,
			}
			p.log.Store(s, newEntry)
		}
	}
}

// leader HandleP1b handles P1b message
func (p *Paxos) HandleP1b(m P1b) {

	p.update(m.Log)

	// old message
	if m.Ballot < p.ballot || p.active {
		// log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false // not necessary
		// forward pending requests to new leader
		p.forward()
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true
			for i := p.execute; i <= p.slot; i++ {
				value, ok := p.log.Load(i)
				if ok {
					if entry, ok := value.(*Entry); ok {
						if !entry.Commit {
							entry.Ballot = p.ballot
							entry.Quorum = paxi.NewQuorum()
							entry.Quorum.ACK(p.ID())
							log.Debugf("wo shi cong follower node zhaode xiaoxi")
							p.Broadcast(P2a{
								Ballot:  p.ballot,
								ID:      p.ID(),
								Slot:    i,
								Command: entry.Command,
								Request: entry.Request,
							})
						}
					}
				}
			}
			// 提议新的请求
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

// Follower's HandleP2a handles P2a message
func (p *Paxos) HandleP2a(m P2a) {
	if m.Ballot >= p.ballot {
		//log.Debugf("HandleP2a: Follower's %s handles P2a message %v\n", p.ID(), m)
		p.ballot = m.Ballot
		p.active = false
		p.slot = m.Slot
		// update slot number
		//p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log.Load(p.slot); exists {
			if entry, ok := e.(*Entry); ok {
				if !entry.Commit && m.Ballot > entry.Ballot {

					if !entry.Command.Equal(m.Command) && entry.Request != nil {
						p.Forward(m.Ballot.ID(), *entry.Request)
						entry.Request = nil
					}
					entry.Command = m.Command
					entry.Ballot = m.Ballot
					entry.Commutativity = m.Commutativity
					entry.Quorum = paxi.NewQuorum()
					entry.Request = m.Request
					entry.Status = m.Status
				}
				entry.Quorum = paxi.NewQuorum()
				entry.Quorum.ACK(p.ID())
				entry.Quorum.ACK(m.ID)
			}
		} else {

			newEntry := &Entry{
				Ballot:        m.Ballot,
				Quorum:        paxi.NewQuorum(),
				Commutativity: m.Commutativity,
				Command:       m.Command,
				Status:        Accept,
				Request:       m.Request,
				Commit:        false,
			}
			newEntry.Quorum.ACK(p.ID())
			newEntry.Quorum.ACK(m.ID)
			p.log.Store(m.Slot, newEntry)
		}
	}
	// if p.Q2(e.Quorum) {
	// 	e.Commit = true
	// 	e.Status = Commit
	// 	p.exec()
	// }
	newEntry := &Entry{
		Ballot:        m.Ballot,
		Commutativity: m.Commutativity,
		Quorum:        paxi.NewQuorum(),
		Command:       m.Command,
		Status:        Accept,
		Request:       m.Request,
		Commit:        false,
	}
	newEntry.Quorum.ACK(p.ID())
	newEntry.Quorum.ACK(m.ID)
	// if 1.3 response to client, use next
	// if !*highload {
	// 	p.Broadcast(P2b{
	// 		Ballot: m.Ballot,
	// 		ID:     p.ID(),
	// 		Slot:   m.Slot,
	// 		Entry:  newEntry,
	// 	})
	// }
	// if other response to client , use next
	// if *highload {
	// 	p.Send("1.3", P2b{
	// 		Ballot: m.Ballot,
	// 		ID:     p.ID(),
	// 		Slot:   m.Slot,
	// 		Entry:  newEntry,
	// 	})
	// } else {
	// 	p.Broadcast(P2b{
	// 		Ballot: m.Ballot,
	// 		ID:     p.ID(),
	// 		Slot:   m.Slot,
	// 		Entry:  newEntry,
	// 	})
	// }
}

// HandleP2b handles P2b message
func (p *Paxos) HandleP2b(m P2b) {
	if m.Slot < p.execute {
		return
	}
	log.Debugf("HandleP2b: Replica %s ==receive p2b with slot=%d from==>>> Replica %s\n", p.ID(), m.Slot, m.ID)
	e, exist := p.log.Load(m.Slot)
	if !exist {

		newEntry := &Entry{
			Ballot:        m.Entry.Ballot,
			Quorum:        m.Entry.Quorum,
			Commutativity: m.Entry.Commutativity,
			Command:       m.Entry.Command,
			Status:        m.Entry.Status,
			Request:       m.Entry.Request,
			Commit:        false,
		}
		newEntry.Quorum.ACK(p.ID())
		p.log.Store(m.Slot, newEntry)
		//
		// if !*highload {
		// 	p.Broadcast(P2b{
		// 		Ballot: m.Ballot,
		// 		ID:     p.ID(),
		// 		Slot:   m.Slot,
		// 		Entry:  newEntry,
		// 	})
		// }

		if p.Q2(newEntry.Quorum) {
			newEntry.Commit = true
			newEntry.Status = Commit
			log.Debugf("Replica %s zou1", p.ID())
			go p.exec(m.Slot)
			go p.Broadcast(P3{
				Slot:    p.execute,
				Command: newEntry.Command,
			})
		}
	} else {
		switch e := e.(type) {
		case *Entry:
			switch e.Status {
			case Accept:
				e.Quorum.ACK(m.ID)
				if p.Q2(e.Quorum) {
					e.Commit = true
					e.Status = Commit
					log.Debugf("Replica %s zou2", p.ID())
					go p.exec(m.Slot)
				}
			case Commit:
				log.Debugf("Replica %s zou3", p.ID())
				go p.exec(m.Slot)
			case Execute:
				return
			}
		}
	}

}

// HandleP3 handles phase 3 commit message
func (p *Paxos) HandleP3(m P3) {
	p.slot = paxi.Max(p.slot, m.Slot)
	var e *Entry

	if value, ok := p.log.Load(m.Slot); ok {
		e := value.(*Entry)
		// if !e.Command.Equal(m.Command) && e.Request != nil {
		// 	p.Forward(m.Ballot.ID(), *e.Request)
		// 	e.Request = nil
		// }
		log.Debugf("e := value.(*Entry)%v\n", e)
	} else {
		p.log.Store(m.Slot, &Entry{})
		value, _ := p.log.Load(m.Slot)
		e = value.(*Entry)
		e.Command = m.Command
		e.Commit = true
	}

	if e != nil {
		e.Command = m.Command
		e.Status = Commit
		e.Commit = true
		p.exec(m.Slot)
	}
}

func (p *Paxos) exec(s int) {
	log.Debugf("Replica %s wants to execute in slot %d, now the p.execute is %d", p.ID(), s, p.execute)

	// Calculate the maximum slot to consider for out-of-order execution
	maxSlot := p.execute + 5

	// Check if s is within the range to consider for out-of-order execution
	if s > p.execute && s <= maxSlot {
		// Check if the slot s has an entry
		e, exist := p.log.Load(s)
		if !exist {
			log.Debugf("Replica %s has a hole in slot %d", p.ID(), s)
			return
		}
		entry := e.(*Entry)
		if entry.Status != Commit {
			return
		}
		// Execute the command
		value := p.Execute(entry.Command)
		entry.Status = Execute
		log.Debugf("Replica %s executes slot %d out-of-order", p.ID(), s)

		// If there is a request associated with the entry, send reply
		if entry.Request != nil && entry.Commutativity {
			reply := paxi.Reply{
				Command:    entry.Command,
				Value:      value,
				Properties: make(map[string]string),
			}
			p.RelpyForward(entry.Command, reply)
			log.Debugf("Reply sent: %v\n", reply)
			entry.Request = nil // Clear the request as it has been processed
		}
		// Do not update p.execute as there might be holes before s
		return
	}

	// If out-of-order execution is not possible or not done, execute in order
	for {
		e, exist := p.log.Load(p.execute)
		if !exist {
			log.Debugf("Replica %s has a hole in slot %d", p.ID(), p.execute)
			break
		}
		entry := e.(*Entry)
		// if entry.Status == Accept {
		// 	log.Debugf("Replica %s's entry is accept in slot %d", p.ID(), p.execute)
		// 	break
		// }
		if entry.Status == Execute {
			log.Debugf("Replica %s's entry is executed in slot %d", p.ID(), p.execute)
			p.execute++
			break
		}
		value := p.Execute(entry.Command)
		entry.Status = Execute
		p.execute++
		log.Debugf("Replica %s executes slot %d in order,next slot is %d", p.ID(), p.execute-1, p.execute)
		if entry.Request != nil {
			reply := paxi.Reply{
				Command:    entry.Command,
				Value:      value,
				Properties: make(map[string]string),
			}
			p.RelpyForward(entry.Command, reply)
			log.Debugf("Reply sent: %v\n", reply)
			entry.Request = nil
		}
		break
	}
}

//available code copy
// func (p *Paxos) exec(s int) {
// 	log.Debugf("Replica %s wants to execute in slot %d, now the ExecuteSlot is %d", p.ID(), s, p.execute)
// 	//tmp := make([]int, 0) //tmp records the slot that has been executed in each round
// 	for {
// 		e, exist := p.log.Load(p.execute)
// 		if !exist {
// 			log.Debugf("Replica %s has a hole in slot %d ", p.ID(), p.execute)
// 			break
// 		}
// 		entry := e.(*Entry)
// 		if entry.Status != Commit {
// 			log.Debugf("Replica %s's entry != committed in slot %d, entry.Status = %v ", p.ID(), s, entry.Status)
// 			break
// 		}
// 		value := p.Execute(entry.Command)
// 		entry.Status = Execute
// 		log.Debugf("Replica %s zhijie execute %d ", p.ID(), p.execute)
// 		if entry.Request != nil {
// 			//log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), s, entry.Command)
// 			reply := paxi.Reply{
// 				Command:    entry.Command,
// 				Value:      value,
// 				Properties: make(map[string]string),
// 			}
// 			p.RelpyForward(entry.Command, reply)
// 			log.Debugf("reply ok%v\n", reply)
// 			entry.Request = nil
// 		}
// 		p.execute++
// 	}
// }

// func (p *Paxos) exec(s int) {
// 	log.Debugf("Replica %s wants to execute in slot %d, now the ExecuteSlot is %d", p.ID(), s, p.ExecuteSlot)
// 	s = p.ExecuteSlot
// 	//tmp := make([]int, 0) //tmp records the slot that has been executed in each round
// 	for {
// 		s++
// 		// if s >= p.ExecuteSlot+*slidewindow {
// 		// 	break
// 		// }
// 		e, exist := p.log.Load(s)
// 		if !exist {
// 			break
// 		}
// 		entry := e.(*Entry)
// 		if entry.Status != Commit {
// 			log.Debugf("Replica %s has a hole in slot %d ", p.ID(), s)
// 			break
// 		}
// 		var value paxi.Value
// 		if s == p.ExecuteSlot+1 && entry.Status == Commit {
// 			entry.Status = Execute
// 			value = p.Execute(entry.Command)
// 			p.ExecuteSlot++
// 			log.Debugf("Replica %s zhijie execute %d ", p.ID(), s)
// 		} else {
// 			if entry.Commutativity && entry.Status == Commit {
// 				value = p.Execute(entry.Command)
// 				entry.Status = Execute
// 				log.Debugf("Replica %s jianjie execute %d ", p.ID(), s)
// 			} else {
// 				switch entry.Status {
// 				case Accept:
// 					// Handle log holes if needed
// 				case Commit:
// 					// Handle log holes if needed
// 				}
// 				break
// 				//todo : Actively pulling data to fill holes
// 			}
// 		}
// 		if entry.Request != nil {
// 			log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), s, entry.Command)
// 			reply := paxi.Reply{
// 				Command:    entry.Command,
// 				Value:      value,
// 				Properties: make(map[string]string),
// 			}
// 			reply.Properties[HTTPHeaderSlot] = strconv.Itoa(p.execute)
// 			reply.Properties[HTTPHeaderBallot] = entry.Ballot.String()
// 			reply.Properties[HTTPHeaderExecute] = strconv.Itoa(p.execute)
// 			log.Debugf("reply ready? %v\n", entry.Request)
// 			p.RelpyForward(entry.Command, reply)
// 			log.Debugf("reply ok\n")
// 			entry.Request = nil
// 		}
// 	}
// }
func (p *Paxos) forward() {
	for _, m := range p.requests {
		p.Forward(p.ballot.ID(), *m)
	}
	p.requests = make([]*paxi.Request, 0)
}
