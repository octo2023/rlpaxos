package paxi

const X int = 2
const K int = 1

// Quorum records each acknowledgement and check for different types of quorum satisfied
type Quorum struct {
	Size  int
	Acks  map[ID]bool
	Zones map[int]int
	Nacks map[ID]bool
}

// NewQuorum returns a new Quorum
func NewQuorum() *Quorum {
	q := &Quorum{
		Size:  0,
		Acks:  make(map[ID]bool),
		Zones: make(map[int]int),
	}
	return q
}

// ACK adds id to quorum ack records
func (q *Quorum) ACK(id ID) {
	if !q.Acks[id] {
		q.Acks[id] = true
		q.Size++
		q.Zones[id.Zone()]++
	}
}

// NACK adds id to quorum nack records
func (q *Quorum) NACK(id ID) {
	if !q.Nacks[id] {
		q.Nacks[id] = true
	}
}

// ADD increase ack size by one
func (q *Quorum) ADD() {
	q.Size++
}

// Size returns current ack size
func (q *Quorum) Sizee() int {
	return q.Size
}

// Reset resets the quorum to empty
func (q *Quorum) Reset() {
	q.Size = 0
	q.Acks = make(map[ID]bool)
	q.Zones = make(map[int]int)
	q.Nacks = make(map[ID]bool)
}

func (q *Quorum) All() bool {
	return q.Size == config.n
}

// Majority quorum satisfied
func (q *Quorum) Majority() bool {
	return q.Size > config.n/2
}

// Majority+K quorum satisfied
func (q *Quorum) MajorityX() bool {
	return q.Size >= (config.n/2)+X
}

// FastQuorum from fast paxos
func (q *Quorum) FastQuorum() bool {
	return q.Size >= config.n*3/4
}

// AllZones returns true if there is at one ack from each zone
func (q *Quorum) AllZones() bool {
	return len(q.Zones) == config.z
}

// ZoneMajority returns true if majority quorum satisfied in any zone
func (q *Quorum) ZoneMajority() bool {
	for z, n := range q.Zones {
		if n > config.npz[z]/2 {
			return true
		}
	}
	return false
}

// GridRow == AllZones
func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

// GridColumn == all nodes in one zone
func (q *Quorum) GridColumn() bool {
	for z, n := range q.Zones {
		if n == config.npz[z] {
			return true
		}
	}
	return false
}

// FGridQ1 is flexible grid quorum for phase 1
func (q *Quorum) FGridQ1(Fz int) bool {
	zone := 0
	for z, n := range q.Zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= config.z-Fz
}

// FGridQ2 is flexible grid quorum for phase 2
func (q *Quorum) FGridQ2(Fz int) bool {
	zone := 0
	for z, n := range q.Zones {
		if n > config.npz[z]/2 {
			zone++
		}
	}
	return zone >= Fz+1
}

/*
// Q1 returns true if config.Quorum type is satisfied
func (q *Quorum) Q1() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size >= config.n-config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}

// Q2 returns true if config.Quorum type is satisfied
func (q *Quorum) Q2() bool {
	switch config.Quorum {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	case "count":
		return q.size > config.F
	default:
		log.Error("Unknown quorum type")
		return false
	}
}
*/
