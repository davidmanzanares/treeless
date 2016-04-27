package servergroup

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
	"treeless/com"
	"treeless/com/protocol"
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

func (sg *ServerGroup) Stop() {
	sg.mutex.Lock()
	for _, s := range sg.servers {
		s.freeConn()
	}
	sg.mutex.Unlock()
}

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
	}
	return sg
}

func Assoc(addr string) (*ServerGroup, error) {
	//Connect to the provided address
	c, err := tlcom.CreateConnection(addr, func() {})
	if err != nil {
		return nil, err
	}
	defer c.Close()
	serialization, err := c.GetAccessInfo()
	if err != nil {
		return nil, err
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
	}

	return sg, nil
}

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.servers)) + " servers:\n"
	t := time.Now()

	var addrs []string
	for k := range sg.servers {
		addrs = append(addrs, k)
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		s := sg.servers[addr]
		dead := ""
		if s.dead {
			dead = "DEAD "
		}
		str += "\t Address: " + s.Phy +
			"\n\t\t" + dead + "Known chunks: " + fmt.Sprint(s.heldChunks) + " Last heartbeat: " + (t.Sub(s.lastHeartbeat)).String() + "\n"
	}

	str += fmt.Sprint(sg.numChunks) + " chunks:\n"
	for i := range sg.chunks {
		srv := "\n\t"
		column := 0
		var holders []string
		for _, h := range sg.chunks[i].holders {
			holders = append(holders, h.Phy)
		}
		sort.Strings(holders)
		for _, phy := range holders {
			if column == 3 {
				srv = srv + "\n\t"
			}
			srv += "\t" + phy
			column++
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
	for _, h := range c.holders {
		holders[i] = h
		i++
	}
	sg.mutex.RUnlock()
	return holders
}

func (sg *ServerGroup) GetAnyHolder(chunkID int) *VirtualServer {
	sg.mutex.RLock()
	if len(sg.chunks[chunkID].holders) < 1 {
		sg.mutex.RUnlock()
		return nil
	}
	h := sg.chunks[chunkID].holders[rand.Intn(len(sg.chunks[chunkID].holders))]
	sg.mutex.RUnlock()
	return h
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
func (sg *ServerGroup) GetServerChunks(addr string) []protocol.AmAliveChunk {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	s, ok := sg.servers[addr]
	if !ok {
		return nil
	}
	c := make([]protocol.AmAliveChunk, len(s.heldChunks))
	copy(c, s.heldChunks)
	return c
}

/*
	ServerGroup setters
*/

func (sg *ServerGroup) SetServerChunks(addr string, cids []protocol.AmAliveChunk) {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()

	s, ok := sg.servers[addr]

	if !ok {
		return
	}
	s.dead = false
	for _, c := range s.heldChunks {
		i := 0
		for ; i < len(cids); i++ {
			if cids[i].ID == c.ID {
				break
			}
		}
		if i == len(cids) {
			//Forgotten chunk
			sg.chunks[c.ID].removeHolder(s)
		}
	}

	for i := 0; i < len(cids); i++ {
		if !sg.chunks[cids[i].ID].hasHolder(s) {
			//Added chunk
			sg.chunks[cids[i].ID].addHolder(s)
		}
	}

	s.heldChunks = cids
	s.lastHeartbeat = time.Now()
}

func (sg *ServerGroup) ServerAlive(addr string) {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	s, ok := sg.servers[addr]
	if !ok {
		return
	}
	s.dead = false
	s.lastHeartbeat = time.Now()
}

func (sg *ServerGroup) IsServerOnGroup(addr string) bool {
	sg.mutex.RLock()
	_, ok := sg.servers[addr]
	sg.mutex.RUnlock()
	return ok

}

func (sg *ServerGroup) AddServerToGroup(addr string) (*VirtualServer, error) {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	s, ok := sg.servers[addr]
	if !ok {
		s = new(VirtualServer)
		sg.servers[addr] = s
		s.Phy = addr
		log.Println("Server", addr, "added")
		return s, nil
	}
	return nil, errors.New("Server already known")
}

func (sg *ServerGroup) RemoveServer(addr string) error {
	sg.mutex.Lock()
	s, ok := sg.servers[addr]
	if !ok {
		sg.mutex.Unlock()
		return errors.New("Server not known")
	}
	delete(sg.servers, addr)
	for _, c := range s.heldChunks {
		sg.chunks[c.ID].removeHolder(s)
	}
	sg.mutex.Unlock()
	s.freeConn()
	return nil
}

func (sg *ServerGroup) DeadServer(addr string) error {
	sg.mutex.Lock()
	s, ok := sg.servers[addr]
	if !ok {
		sg.mutex.Unlock()
		return errors.New("Server not known")
	}
	if !s.dead {
		log.Println("Server is dead:", addr)
		s.dead = true
	}
	for _, c := range s.heldChunks {
		sg.chunks[c.ID].removeHolder(s)
	}
	s.heldChunks = nil
	sg.mutex.Unlock()
	s.freeConn()
	return nil
}

func (sg *ServerGroup) IsSynched(cid int) bool {
	sg.mutex.RLock()
	c := sg.chunks[cid]
	if len(c.holders) < 2 {
		sg.mutex.RUnlock()
		return true
	}
	sum := c.holders[0].getChunk(cid).Checksum
	for i := 1; i < len(c.holders); i++ {
		h := c.holders[i]
		sum2 := h.getChunk(cid).Checksum
		if sum != sum2 {
			//log.Println(c.holders[0].Phy, sum, c.holders[i].Phy, sum2)
			sg.mutex.RUnlock()
			return false
		}
	}
	sg.mutex.RUnlock()
	return true
}

func (sg *ServerGroup) UnSynchedChunks() []int {
	var list []int
	for i := range sg.chunks {
		if !sg.IsSynched(i) {
			list = append(list, i)
		}
	}
	return list
}
