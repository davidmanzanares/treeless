package tlsg

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

//Hide virtuals

//ServerGroup provides an access to a DB server group
type ServerGroup struct {
	mutex sync.RWMutex //All ServerGroup read/writes are mutex-protected
	//Database configuration
	NumChunks  int //Number of DB chunks
	Redundancy int //DB target redundancy
	//External status
	Servers map[string]*VirtualServer //Set of all DB servers
	chunks  []VirtualChunk            //Array of all DB chunks
}

/*
	ServerGroup utils
*/

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	//DB configuration
	sg.NumChunks = numChunks
	sg.Redundancy = redundancy
	//Initialization
	sg.Servers = make(map[string]*VirtualServer)
	sg.chunks = make([]VirtualChunk, sg.NumChunks)
	//Add all chunks to the servergroup
	for i := 0; i < sg.NumChunks; i++ {
		sg.chunks[i].ID = i
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
	}
	return sg
}

//UnmarhalServerGroup unmarshalles serialization creating a new ServerGroup
//This ServerGroup will be configured (NumChunks and Redundancy), and it will have
//a server list copy. It won't contain any chunk information.
func UnmarhalServerGroup(serialization []byte) (*ServerGroup, error) {
	sg := new(ServerGroup)
	err := sg.unmarshal(serialization)
	if err != nil {
		return nil, err
	}
	sg.chunks = make([]VirtualChunk, sg.NumChunks)
	//Add all chunks to the servergroup
	for i := 0; i < sg.NumChunks; i++ {
		sg.chunks[i].ID = i
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
	}

	return sg, nil
}

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.Servers)) + " servers:\n"
	t := time.Now()
	for _, s := range sg.Servers {
		str += "\t Address: " + s.Phy +
			"\n\t\tKnown chunks: " + fmt.Sprint(s.heldChunks) + " Last heartbeat: " + (t.Sub(s.lastHeartbeat)).String() + "\n"
	}
	str += fmt.Sprint(sg.NumChunks) + " chunks:\n"
	for i := range sg.chunks {
		srv := ""
		for k := range sg.chunks[i].Holders {
			srv += "\n\t\t" + k.Phy
		}
		str += "\tChunk " + fmt.Sprint(i) + srv + "\n"
	}
	return str
}

//Marshal DB configuration and server addresses
func (sg *ServerGroup) Marshal() ([]byte, error) {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	return json.Marshal(sg)
}

func (sg *ServerGroup) unmarshal(b []byte) error {
	return json.Unmarshal(b, sg)
}

/*
	ServerGroup getters
*/

func (sg *ServerGroup) NumHolders(chunkID int) int {
	sg.mutex.RLock()
	num := len(sg.chunks[chunkID].Holders)
	sg.mutex.RUnlock()
	return num
}

func (sg *ServerGroup) GetChunkHolders(chunkID int) (holders [8]*VirtualServer) {
	sg.mutex.RLock()
	c := sg.chunks[chunkID]
	i := 0
	for h := range c.Holders {
		holders[i] = h
		i++
	}
	sg.mutex.RUnlock()
	return holders
}

func (sg *ServerGroup) GetAnyHolder(chunkID int) *VirtualServer {
	sg.mutex.RLock()
	for k := range sg.chunks[chunkID].Holders {
		sg.mutex.RUnlock()
		return k
	}
	sg.mutex.RUnlock()
	return nil
}

func (sg *ServerGroup) KnownServers() []string {
	var list []string
	sg.mutex.RLock()
	for k := range sg.Servers {
		list = append(list, k)
	}
	sg.mutex.RUnlock()
	return list
}

/*
	ServerGroup setters
*/

func (sg *ServerGroup) SetServerChunks(addr string, cids []int) []int {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()

	var changes []int
	s := sg.Servers[addr]

	for c := range s.heldChunks {
		i := 0
		for ; i < len(cids); i++ {
			if cids[i] == c {
				break
			}
		}
		if i == len(cids) {
			//Forgotten chunk
			delete(sg.chunks[c].Holders, s)
			changes = append(changes, c)
		}
	}

	for i := 0; i < len(cids); i++ {
		if !sg.chunks[cids[i]].Holders[s] {
			//Added chunk
			sg.chunks[cids[i]].Holders[s] = true
			changes = append(changes, cids[i])
		}
	}

	s.heldChunks = cids
	s.lastHeartbeat = time.Now()
	return changes
}

func (sg *ServerGroup) AddServerToGroup(addr string) error {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	s, ok := sg.Servers[addr]
	if !ok {
		s = new(VirtualServer)
		sg.Servers[addr] = s
		s.Phy = addr
		log.Println("Server", addr, "added")
		return nil
	}
	return errors.New("Server already known")
}
