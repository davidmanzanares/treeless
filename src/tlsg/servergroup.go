package tlsg

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"treeless/src/tlcom"
)

//Hide virtuals

type serializableServerGroup struct {
	NumChunks  int //Number of DB chunks
	Redundancy int //DB target redundancy
	Servers    map[string]*VirtualServer
}

//ServerGroup provides an access to a DB server group
type ServerGroup struct {
	mutex sync.RWMutex //All ServerGroup read/writes are mutex-protected
	//Database configuration
	numChunks  int //Number of DB chunks
	redundancy int //DB target redundancy
	//External status
	servers map[string]*VirtualServer //Set of all DB servers
	chunks  []VirtualChunk            //Array of all DB chunks
}

/*
	ServerGroup utils
*/

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	//DB configuration
	sg.numChunks = numChunks
	sg.redundancy = redundancy
	//Initialization
	sg.servers = make(map[string]*VirtualServer)
	sg.chunks = make([]VirtualChunk, sg.numChunks)
	//Add all chunks to the servergroup
	for i := 0; i < sg.numChunks; i++ {
		sg.chunks[i].id = i
		sg.chunks[i].holders = make(map[*VirtualServer]bool)
	}
	return sg
}

func Assoc(addr string) (*ServerGroup, error) {
	//Connect to the provided address
	c, err := tlcom.CreateConnection(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	serialization, err := c.GetAccessInfo()
	if err != nil {
		panic(err)
	}
	return UnmarhalServerGroup(serialization)
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
	sg.chunks = make([]VirtualChunk, sg.numChunks)
	//Add all chunks to the servergroup
	for i := 0; i < sg.numChunks; i++ {
		sg.chunks[i].id = i
		sg.chunks[i].holders = make(map[*VirtualServer]bool)
	}

	return sg, nil
}

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.servers)) + " servers:\n"
	t := time.Now()
	for _, s := range sg.servers {
		str += "\t Address: " + s.Phy +
			"\n\t\tKnown chunks: " + fmt.Sprint(s.heldChunks) + " Last heartbeat: " + (t.Sub(s.lastHeartbeat)).String() + "\n"
	}
	str += fmt.Sprint(sg.numChunks) + " chunks:\n"
	for i := range sg.chunks {
		srv := ""
		for k := range sg.chunks[i].holders {
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
	ssg := new(serializableServerGroup)
	ssg.NumChunks = sg.numChunks
	ssg.Redundancy = sg.redundancy
	ssg.Servers = sg.servers
	return json.Marshal(ssg)
}

func (sg *ServerGroup) unmarshal(b []byte) error {
	ssg := new(serializableServerGroup)
	err := json.Unmarshal(b, ssg)
	if err != nil {
		return err
	}
	sg.numChunks = ssg.NumChunks
	sg.redundancy = ssg.Redundancy
	sg.servers = ssg.Servers
	return nil
}

/*
	ServerGroup getters
*/

func (sg *ServerGroup) NumChunks() int {
	return sg.numChunks
}

func (sg *ServerGroup) Redundancy() int {
	sg.mutex.RLock()
	r := sg.redundancy
	sg.mutex.RUnlock()
	return r
}

func (sg *ServerGroup) NumServers() int {
	sg.mutex.RLock()
	r := len(sg.servers)
	sg.mutex.RUnlock()
	return r
}

func (sg *ServerGroup) NumHolders(chunkID int) int {
	sg.mutex.RLock()
	num := len(sg.chunks[chunkID].holders)
	sg.mutex.RUnlock()
	return num
}

func (sg *ServerGroup) Servers() []*VirtualServer {
	sg.mutex.RLock()
	l := make([]*VirtualServer, 0, len(sg.servers))
	for _, s := range sg.servers {
		l = append(l, s)
	}
	sg.mutex.RUnlock()
	return l
}

func (sg *ServerGroup) GetChunkHolders(chunkID int) (holders [8]*VirtualServer) {
	sg.mutex.RLock()
	c := sg.chunks[chunkID]
	i := 0
	for h := range c.holders {
		holders[i] = h
		i++
	}
	sg.mutex.RUnlock()
	return holders
}

func (sg *ServerGroup) GetAnyHolder(chunkID int) *VirtualServer {
	sg.mutex.RLock()
	for k := range sg.chunks[chunkID].holders {
		sg.mutex.RUnlock()
		return k
	}
	sg.mutex.RUnlock()
	return nil
}

func (sg *ServerGroup) KnownServers() []string {
	var list []string
	sg.mutex.RLock()
	for k := range sg.servers {
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
	s := sg.servers[addr]

	for c := range s.heldChunks {
		i := 0
		for ; i < len(cids); i++ {
			if cids[i] == c {
				break
			}
		}
		if i == len(cids) {
			//Forgotten chunk
			delete(sg.chunks[c].holders, s)
			changes = append(changes, c)
		}
	}

	for i := 0; i < len(cids); i++ {
		if !sg.chunks[cids[i]].holders[s] {
			//Added chunk
			sg.chunks[cids[i]].holders[s] = true
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
	s, ok := sg.servers[addr]
	if !ok {
		s = new(VirtualServer)
		sg.servers[addr] = s
		s.Phy = addr
		log.Println("Server", addr, "added")
		return nil
	}
	return errors.New("Server already known")
}
