package paxos2bro

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// CommandBallot conbines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	ID            paxi.ID
	Ballot        paxi.Ballot
	Slot          int
	Commutativity bool //
	Command       paxi.Command
	Request       *paxi.Request
	Status        Status
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
	Entry  *Entry
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d Entry=%v}", m.Ballot, m.ID, m.Slot, m.Entry)
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// pull log hole message
type Pullrequest struct {
	ID   paxi.ID
	Slot []int
}

func (p Pullrequest) String() string {
	return fmt.Sprintf("Pullrequest { s=%d }", p.Slot)
}

// leader push log message
type Pushrequest struct {
	ID        paxi.ID
	PushEntry map[int]*Entry
}

func (p Pushrequest) String() string {
	return fmt.Sprintf("Pullrequest { s=%s, PushEntry=%v }", p.ID, p.PushEntry)
}
