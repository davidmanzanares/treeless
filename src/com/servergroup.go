package tlcom

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"treeless/src/com/lowcom"
)

//ServerGroup provides an access to a DB server group
type ServerGroup struct {
	//All ServerGroup read/writes are mutex-protected
	sync.Mutex
	//External things
	Redundancy int                       //DB target redundancy
	Servers    map[string]*VirtualServer //Set of all DB servers
	NumChunks  int
	//Local things
	stopped            bool
	localhost          *VirtualServer
	chunkUpdateChannel chan *VirtualChunk //Each time a chunk status is updated the chunk is sent to the channel
	heartbeatList      *list.List         //List of pending chunk reviews
	chunks             []VirtualChunk     //Array of all DB chunks
	chunkStatus        []ChunkStatus      //Array of all DB chunks status
}

const channelUpdateChannelBufferSize = 1024

type ChunkStatus int64

const (
	ChunkNotPresent ChunkStatus = iota
	ChunkSynched
	ChunkNotSynched
)

func (sg *ServerGroup) Stop() {
	sg.Mutex.Lock()
	sg.stopped = true
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

//Marshal number of chunks and servers physical address
func (sg *ServerGroup) Marshal() ([]byte, error) {
	sg.Lock()
	defer sg.Unlock()
	return json.Marshal(sg)
}

//Unmarshal number of chunks and servers physical address
func (sg *ServerGroup) Unmarshal(b []byte) error {
	err := json.Unmarshal(b, sg)
	if err != nil {
		return err
	}
	for i := range sg.chunks {
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
	}
	return nil
}

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, port string, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	sg.Redundancy = redundancy
	sg.Servers = make(map[string]*VirtualServer)
	sg.NumChunks = numChunks
	sg.chunkUpdateChannel = make(chan *VirtualChunk, channelUpdateChannelBufferSize)
	sg.chunkStatus = make([]ChunkStatus, numChunks)

	//Add this server
	localhost := new(VirtualServer)
	localhost.LastHeartbeat = time.Now()
	localhost.Phy = tlLowCom.GetLocalIP() + ":" + port
	localhost.HeldChunks = make([]int, numChunks)
	for i := 0; i < numChunks; i++ {
		//Add all chunks to this server
		localhost.HeldChunks[i] = i
	}
	sg.localhost = localhost
	sg.Servers[localhost.Phy] = localhost

	//Add all chunks to the servergroup
	sg.chunks = make([]VirtualChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		sg.chunks[i].ID = i
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
		sg.chunks[i].Holders[localhost] = true
	}
	//Set chunk status
	for i := 0; i < numChunks; i++ {
		sg.chunkStatus[i] = ChunkSynched
	}
	heartbeatRequester(sg)
	return sg
}

//ConnectAsClient connects to an existing server group as a client
func ConnectAsClient(addr string) (*ServerGroup, error) {
	//Connect to the provided address
	c, err := CreateConnection(addr)
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
	sg.chunks = make([]VirtualChunk, sg.NumChunks)
	for i := range sg.chunks {
		sg.chunks[i].ID = i
		sg.chunks[i].Holders = make(map[*VirtualServer]bool)
	}
	sg.chunkStatus = make([]ChunkStatus, sg.NumChunks)
	//Initial access configuration has been loaded
	//Lookup for servers
	for _, s := range sg.Servers {
		//Request known chunks list
		b, err := tlLowCom.UDPRequest(s.Phy, time.Millisecond*50)
		if err == nil {
			var udpr Keepalive
			err = json.Unmarshal(b, &udpr)
			if err != nil {
				log.Println("UDPResponse unmarshalling error", err)
				continue
			}
			s.LastHeartbeat = time.Now()
			s.HeldChunks = udpr.KnownChunks
			for i := range s.HeldChunks {
				sg.chunks[i].Holders[s] = true
			}
		} else {
			log.Println("UDP initial request transmission error", err)
		}
	}
	sg.chunkUpdateChannel = make(chan *VirtualChunk, 1024)
	//Launch UDP heartbeat requester
	heartbeatRequester(sg)
	//All existing chunks must be checked
	for i := range sg.chunks {
		sg.chunkUpdateChannel <- &sg.chunks[i]
	}
	return sg, nil
}

//Associate connects to an existing server group and adds this server to it
func Associate(destAddr string, localport string) (*ServerGroup, error) {
	//Create a client connection first
	sg, err := ConnectAsClient(destAddr)
	if err != nil {
		panic(err)
	}
	sg.Lock()
	defer sg.Unlock()
	localhost := tlLowCom.GetLocalIP() + ":" + localport
	//Add to external servergroup instances
	//For each other server: add localhost
	for _, s := range sg.Servers {
		err := s.NeedConnection()
		if err != nil {
			log.Println("TCP transmission error when trying to add server", err)
			continue
		}
		err = s.Conn.AddServerToGroup(localhost)
		if err != nil {
			panic(err)
		}
	}
	//Add to local servergroup instance
	sg.Unlock()
	sg.AddServerToGroup(localhost)
	sg.Lock()
	sg.localhost = sg.Servers[localhost]
	return sg, nil
}

func heartbeatRequester(sg *ServerGroup) {
	d := time.Second * 1
	l := list.New()
	sg.heartbeatList = l
	for _, s := range sg.Servers {
		l.PushFront(s)
	}
	sg.Lock()
	go func() {
		for {
			sg.Unlock()
			time.Sleep(d)
			sg.Lock()

			if sg.stopped {
				return
			}

			s := l.Front().Value.(*VirtualServer)

			//Request known chunks list
			b, err := tlLowCom.UDPRequest(s.Phy, time.Millisecond*50)
			if err == nil {
				//Unmarshal response
				var udpr Keepalive
				err = json.Unmarshal(b, &udpr)
				if err != nil {
					panic(err)
				}
				//Detect added servers
				for _, addr := range udpr.KnownServers {
					_, exists := sg.Servers[addr]
					if !exists {
						sg.Servers[addr] = new(VirtualServer)
						sg.Servers[addr].Phy = addr
						l.PushFront(sg.Servers[addr])
					}
				}
				//Detect added chunks
				for _, c := range udpr.KnownChunks {
					ok := false
					for _, c2 := range s.HeldChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.chunkUpdateChannel <- &sg.chunks[c]
					}
				}
				//Detect forgotten chunks
				for _, c := range s.HeldChunks {
					ok := false
					for _, c2 := range udpr.KnownChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.chunkUpdateChannel <- &sg.chunks[c]
					}
				}

				s.HeldChunks = udpr.KnownChunks
				//log.Println("held", s.Phy, udpr)
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
		//check for dead servers
		//remove them
	}
}

func (sg *ServerGroup) IsChunkPresent(id int) bool {
	return atomic.LoadInt64((*int64)(&sg.chunkStatus[id])) != 0
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
