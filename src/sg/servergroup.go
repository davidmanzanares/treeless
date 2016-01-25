package tlsg

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"treeless/src/com"
	"treeless/src/com/udp"
)

//ServerGroup provides an access to a DB server group
type ServerGroup struct {
	sync.Mutex //All ServerGroup read/writes are mutex-protected
	//Database configuration
	NumChunks  int //Number of DB chunks
	Redundancy int //DB target redundancy
	//External status
	Servers map[string]*VirtualServer //Set of all DB servers
	chunks  []VirtualChunk            //Array of all DB chunks
	//Local properties
	stopped       bool
	localhost     *VirtualServer //Only for servers
	chunkStatus   []ChunkStatus  //Array of all DB chunks status
	heartbeatList *list.List     //List of pending heartbeats to request
	//Rebalance data
	useChannel         bool               //Send chunk updates to chunkUpdateChannel channel?
	chunkUpdateChannel chan *VirtualChunk //Each time a chunk status is updated the chunk is sent to the channel
}

const channelUpdateBufferSize = 1024

var heartbeatTickDuration = time.Second * 1
var heartbeatTimeout = time.Millisecond * 50

type ChunkStatus int64

const (
	ChunkPresent ChunkStatus = 1 << iota
	ChunkSynched
	ChunkWriteable
)

func (sg *ServerGroup) initChunks() {
	sg.chunks = make([]VirtualChunk, sg.NumChunks)
	sg.chunkStatus = make([]ChunkStatus, sg.NumChunks)

	//Add all chunks to the servergroup
	for i := 0; i < sg.NumChunks; i++ {
		sg.chunks[i].ID = i
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
	}
}

func (sg *ServerGroup) initLocalhost(port int) {
	localhost := tlcom.GetLocalIP() + ":" + fmt.Sprint(port)
	s := new(VirtualServer)
	sg.Servers[localhost] = s
	s.Phy = localhost
	s.LastHeartbeat = time.Now()
	sg.localhost = sg.Servers[localhost]
}

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, port int, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	sg.Lock()
	defer sg.Unlock()
	//DB configuration
	sg.NumChunks = numChunks
	sg.Redundancy = redundancy
	//Initialization
	sg.Servers = make(map[string]*VirtualServer)
	sg.initChunks()

	//Start daemons
	sg.StartRebalance()
	sg.startHeartbeatRequester()

	sg.initLocalhost(port)
	//Localhost holds all chunks
	sg.localhost.HeldChunks = make([]int, numChunks)
	for i := 0; i < numChunks; i++ {
		sg.localhost.HeldChunks[i] = i
		sg.chunks[i].Holders[sg.localhost] = true
		sg.chunkStatus[i] = ChunkSynched
	}
	return sg
}

func baseConnect(addr string) (*ServerGroup, error) {
	//Connect to the provided address
	c, err := tlcom.CreateConnection(addr)
	if err != nil {
		return nil, err
	}
	//Request access configuration information: server ips & DB numChunks
	b, err2 := c.GetAccessInfo()
	if err2 != nil {
		return nil, err2
	}
	c.Close()
	//Create a new server group by unmarshaling the provided information
	sg := new(ServerGroup)
	err = sg.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	//Chunk initialization
	sg.initChunks()
	//Lookup for servers
	for _, s := range sg.Servers {
		//Request known chunks list
		aa, err := tlUDP.Request(s.Phy, time.Millisecond*50)
		if err == nil {
			s.LastHeartbeat = time.Now()
			s.HeldChunks = aa.KnownChunks
			for i := range s.HeldChunks {
				sg.chunks[i].Holders[s] = true
			}
		} else {
			log.Println("UDP initial request transmission error", err)
		}
	}
	return sg, nil
}

//ConnectAsClient connects to an existing server group as a client
func ConnectAsClient(addr string) (*ServerGroup, error) {
	sg, err := baseConnect(addr)
	if err != nil {
		return nil, err
	}
	sg.Lock()
	defer sg.Unlock()
	//Launch UDP heartbeat requester
	sg.startHeartbeatRequester()
	return sg, nil
}

//Associate connects to an existing server group and adds this server to it
func Associate(destAddr string, localport int) (*ServerGroup, error) {
	sg, err := baseConnect(destAddr)
	if err != nil {
		return nil, err
	}
	sg.Lock()
	defer sg.Unlock()

	//Start daemons
	sg.StartRebalance()
	sg.startHeartbeatRequester()

	//Add localhost
	sg.initLocalhost(localport)
	//Add to external servergroup instances
	//For each other server: add localhost
	for _, s := range sg.Servers {
		if s == sg.localhost {
			continue
		}
		err := s.NeedConnection()
		if err != nil {
			log.Println("TCP transmission error when trying to add server", err)
			continue
		}
		err = s.Conn.AddServerToGroup(sg.localhost.Phy)
		if err != nil {
			panic(err)
		}
	}

	//All existing chunks must be checked
	for i := range sg.chunks {
		sg.Unlock()
		sg.chunkUpdateChannel <- &sg.chunks[i]
		sg.Lock()
	}

	return sg, nil
}

func (sg *ServerGroup) startHeartbeatRequester() {
	l := list.New()
	sg.heartbeatList = l
	for _, s := range sg.Servers {
		l.PushFront(s)
	}
	go func() {
		sg.Lock()
		for !sg.stopped {
			sg.Unlock()
			time.Sleep(heartbeatTickDuration)
			sg.Lock()
			if l.Len() == 0 {
				continue
			}
			s := l.Front().Value.(*VirtualServer)
			//Request known chunks list
			aa, err := tlUDP.Request(s.Phy, heartbeatTimeout)
			if err == nil {
				//Detect added servers
				for _, addr := range aa.KnownServers {
					_, exists := sg.Servers[addr]
					if !exists {
						sg.Servers[addr] = new(VirtualServer)
						sg.Servers[addr].Phy = addr
						l.PushFront(sg.Servers[addr])
					}
				}
				//Detect added chunks
				for _, c := range aa.KnownChunks {
					ok := false
					for _, c2 := range s.HeldChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok && sg.useChannel {
						sg.Unlock()
						sg.chunkUpdateChannel <- &sg.chunks[c]
						sg.Lock()
					}
				}
				//Detect forgotten chunks
				for _, c := range s.HeldChunks {
					ok := false
					for _, c2 := range aa.KnownChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok && sg.useChannel {
						sg.Unlock()
						sg.chunkUpdateChannel <- &sg.chunks[c]
						sg.Lock()
					}
				}

				s.HeldChunks = aa.KnownChunks
				//log.Println("held", s.Phy, aa)
				//Remove old chunks
				for c := range s.HeldChunks {
					delete(sg.chunks[c].Holders, s)
				}
				//Add new chunks
				for _, c := range s.HeldChunks {
					sg.chunks[c].Holders[s] = true
				}
				s.LastHeartbeat = time.Now()
			} else {
				log.Println("UDP request timeout. Server", s.Phy, "UDP error:", err)
			}
			//log.Println(sg)
			l.MoveToBack(l.Front())
		}
	}()
}

func (sg *ServerGroup) AddServerToGroup(addr string) error {
	sg.Lock()
	defer sg.Unlock()
	s, ok := sg.Servers[addr]
	if !ok {
		s = new(VirtualServer)
		sg.Servers[addr] = s
		s.Phy = addr
		s.LastHeartbeat = time.Now()
		sg.heartbeatList.PushBack(s)
	} else {
		s.LastHeartbeat = time.Now()
	}
	log.Println("Server", addr, "added")
	return nil
}

func oldServersRemover() {
	for {
		time.Sleep(time.Minute)
		//TODO: check for dead servers
		//remove them
	}
}

func (sg *ServerGroup) IsChunkPresent(id int) bool {
	return atomic.LoadInt64((*int64)(&sg.chunkStatus[id])) != 0
}

func (sg *ServerGroup) ChunkStatus(id int) ChunkStatus {
	return ChunkStatus(atomic.LoadInt64((*int64)(&sg.chunkStatus[id])))
}

func (sg *ServerGroup) GetChunkHolders(chunkID int) []*VirtualServer {
	sg.Lock()
	defer sg.Unlock()
	c := sg.chunks[chunkID]
	holders := make([]*VirtualServer, len(c.Holders))
	i := 0
	for h := range c.Holders {
		holders[i] = h
		i++
	}
	return holders
}

func (sg *ServerGroup) Stop() {
	sg.Mutex.Lock()
	sg.stopped = true
	for _, s := range sg.Servers {
		if s.Conn != nil {
			s.Conn.Close()
			s.Conn = nil
		}
	}
	sg.Mutex.Unlock()
}

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.Servers)) + " servers:\n"
	t := time.Now()
	for _, s := range sg.Servers {
		str += "\t Address: " + s.Phy +
			"\n\t\tKnown chunks: " + fmt.Sprint(s.HeldChunks) + " Last heartbeat: " + (t.Sub(s.LastHeartbeat)).String() + "\n"
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
	sg.Lock()
	defer sg.Unlock()
	return json.Marshal(sg)
}

//Unmarshal DB configuration and server addresses
func (sg *ServerGroup) Unmarshal(b []byte) error {
	return json.Unmarshal(b, sg)
}
