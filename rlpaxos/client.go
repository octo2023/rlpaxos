package paxos2bro

import (
	"strconv"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

// Client overwrites read operation for Paxos
type Client struct {
	*paxi.HTTPClient
	ballot paxi.Ballot
}

func NewClient(id paxi.ID) *Client {
	return &Client{
		HTTPClient: paxi.NewHTTPClient(id),
	}
}

// Get implements paxi.Client interface
// there are three reading modes:
// (1) read as normal command
// (2) read from leader with current ballot number
// (3) read from quorum of replicas with barrier
func (c *Client) Get(key paxi.Key) (paxi.Value, error) {

	c.HTTPClient.CID++
	switch *read2bro {
	case "leader":
		return c.readLeader(key)
	case "local":
		return c.readLocal(key)
	case "quorum":
		return c.readQuorum(key)
	case "any":
		return c.readAny(key)
	case "RFL":
		return c.readfollowerlinearizable(key)
	default:
		return c.HTTPClient.Get(key)
	}
}

func (c *Client) Put(key paxi.Key, value paxi.Value) error {
	c.HTTPClient.CID++
	_, _, err := c.RESTPut(c.ID, key, value)
	// if err == nil {
	// 	b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
	// 	if b > c.ballot {
	// 		c.ballot = b
	// 	}
	// }

	return err
}

func (c *Client) readLeader(key paxi.Key) (paxi.Value, error) {
	if c.ballot == 0 {
		v, meta, err := c.HTTPClient.RESTGet(c.ID, key)
		c.ballot = paxi.NewBallotFromString(meta[HTTPHeaderBallot])
		return v, err
	}
	// check ballot number
	v, meta, err := c.HTTPClient.RESTGet(c.ballot.ID(), key)
	b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
	if b > c.ballot {
		c.ballot = b
	}
	return v, err
}
func (c *Client) readLocal(key paxi.Key) (paxi.Value, error) {
	if c.ballot == 0 {
		v, meta, err := c.HTTPClient.RESTGet(c.ID, key)
		c.ballot = paxi.NewBallotFromString(meta[HTTPHeaderBallot])
		return v, err
	}
	// check ballot number
	v, meta, err := c.HTTPClient.RESTGet(c.ballot.ID(), key)
	b := paxi.NewBallotFromString(meta[HTTPHeaderBallot])
	if b > c.ballot {
		c.ballot = b
	}
	return v, err
}
func (c *Client) readQuorum(key paxi.Key) (paxi.Value, error) {
	majority := c.N/2 + 1
	barrier := -1
	numReachedBarrier := 0
	numInProgress := 0
	var value paxi.Value

	// quorum read
	values, metadatas := c.QuorumGet(key)
	for i, v := range values {
		slot, err := strconv.Atoi(metadatas[i][HTTPHeaderSlot])
		if err != nil {
			log.Error(err)
			continue
		}
		inProgress, err := strconv.ParseBool(metadatas[i][HTTPHeaderInProgress])
		if err != nil {
			log.Error(err)
			continue
		}
		if inProgress {
			numInProgress++
		}
		if slot > barrier {
			barrier = slot
			numReachedBarrier = 1
			value = v
		} else if slot == barrier {
			numReachedBarrier++
		}
	}

	// wait for slot to be executed by any node
	for numInProgress > 0 && numReachedBarrier < majority {
		// read from random node
		_, metadata, err := c.HTTPClient.RESTGet("", key)
		if err != nil {
			return nil, err
		}
		// get executed slot
		execute, err := strconv.Atoi(metadata[HTTPHeaderExecute])
		if err != nil {
			log.Error(err)
			continue
		}
		if execute >= barrier {
			break
		}

		// get highest accepted slot
		slot, err := strconv.Atoi(metadata[HTTPHeaderSlot])
		if err != nil {
			log.Error(err)
			continue
		}
		if slot >= barrier {
			numReachedBarrier++
		}
	}

	return value, nil
}

func (c *Client) readAny(key paxi.Key) (paxi.Value, error) {
	v, _, err := c.HTTPClient.RESTGet(c.ID, key)
	return v, err
}

func (c *Client) readfollowerlinearizable(key paxi.Key) (paxi.Value, error) {
	majority := c.N/2 + 1
	barrier := -1
	numReachedBarrier := 0
	numInProgress := 0

	keybarrier := -1
	numkeyReachBarrier := 0
	var slotstatus [][]interface{}
	var nodehole map[paxi.ID]map[int]int
	var value paxi.Value
	// quorum read
	values, metadatas := c.QuorumGet(key)
	for i, v := range values {
		var decodedMap map[int]int
		//maxslot, err := strconv.Atoi(metadatas[i][HTTPHeaderSlot])
		readslot, _ := strconv.Atoi(metadatas[i][HTTPHeaderKeySlot])
		status := metadatas[i][HTTPHeaderkeyStatus]
		slotstatus = append(slotstatus, []interface{}{readslot, status})
		nodeid := metadatas[i][HTTPHeaderNodeID]
		// todo : active pull log hole
		// loghole: a node's hole, decoded
		// loghole := metadatas[i][HTTPHeaderHole]
		// //err := json.Unmarshal([]byte(loghole), &decodedMap)
		// if err != nil {
		// 	log.Error(err)
		// 	return nil, err
		// }
		nodehole[paxi.ID(nodeid)] = decodedMap
		if readslot > keybarrier {
			keybarrier = readslot
			numkeyReachBarrier = 1
			value = v
		} else if readslot == keybarrier {
			numkeyReachBarrier++
		}
	}
	if numkeyReachBarrier >= majority {
		return value, nil
	}
	var filteredSlotStatus [][]interface{}
	for _, tuple := range slotstatus {
		readslott := tuple[0]
		if readslott == keybarrier {
			filteredSlotStatus = append(filteredSlotStatus, tuple)
			if tuple[1] == "executed" && tuple[0] == keybarrier {
				return value, nil
			}
		}
	}
	// //logholemapJson : all node's hole, decoded
	// logholemapJson, err := json.Marshal(nodehole)
	// if err != nil {
	// 	return nil, err
	// }
	// wait for slot to be executed by any node
	for numInProgress > 0 && numReachedBarrier < majority {
		// read from random node
		_, meta, err := c.HTTPClient.RESTGet("", key)
		if err != nil {
			return nil, err
		}
		// get executed slot
		execute, err := strconv.Atoi(meta[HTTPHeaderExecute])
		if err != nil {
			log.Error(err)
			continue
		}
		if execute >= barrier {
			break
		}

		// get highest accepted slot
		slot, err := strconv.Atoi(meta[HTTPHeaderSlot])
		if err != nil {
			log.Error(err)
			continue
		}
		if slot >= barrier {
			numReachedBarrier++
		}
	}

	return value, nil
}
