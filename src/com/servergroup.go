package tlcom

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"treeless/src/com/lowcom"
)

//ServerGroup provides an access to a DB server group
type ServerGroup struct {
	//External things
	sync.Mutex                                   //All ServerGroup read/writes are mutex-protected
	Redundancy         int                       //DB target redundancy
	Servers            map[string]*VirtualServer //Set of all DB servers
	Chunks             []VirtualChunk            //Array of all DB chunks
	Localhost          *VirtualServer
	ChunkUpdateChannel chan *VirtualChunk //Each time a chunk status is updated the chunk is sent to the channel
	//Local things
	heartbeatList *list.List //List of pending chunk reviews
}

const channelUpdateChannelBufferSize = 1024

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.Servers)) + " servers:\n"
	t := time.Now()
	for _, s := range sg.Servers {
		str += "\t Address: " + s.Phy +
			"\n\t\tKnown chunks: " + fmt.Sprint(s.HeldChunks) + " Last heartbeat: " + (t.Sub(s.LastHeartbeat)).String() + "\n"
	}
	str += fmt.Sprint(len(sg.Chunks)) + " chunks:\n"
	for i := range sg.Chunks {
		srv := ""
		for k := range sg.Chunks[i].Holders {
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
	for i := range sg.Chunks {
		sg.Chunks[i].Holders = make(map[*VirtualServer]bool)
	}
	return nil
}

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, port string, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	sg.Redundancy = redundancy
	sg.Servers = make(map[string]*VirtualServer)
	sg.ChunkUpdateChannel = make(chan *VirtualChunk, channelUpdateChannelBufferSize)

	//Add this server
	localhost := new(VirtualServer)
	localhost.LastHeartbeat = time.Now()
	localhost.Phy = tlLowCom.GetLocalIP() + ":" + port
	localhost.HeldChunks = make([]int, numChunks)
	for i := 0; i < numChunks; i++ {
		//Add all chunks to this server
		localhost.HeldChunks[i] = i
	}
	sg.Localhost = localhost
	sg.Servers[localhost.Phy] = localhost

	//Add all chunks to the servergroup
	sg.Chunks = make([]VirtualChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		sg.Chunks[i].ID = i
		sg.Chunks[i].Holders = make(map[*VirtualServer]bool)
		sg.Chunks[i].Holders[localhost] = true
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
	//Initial access configuration has been loaded
	//Lookup for servers
	for _, s := range sg.Servers {
		//Request known chunks list
		b, err := tlLowCom.UDPRequest(s.Phy, time.Millisecond*50)
		if err == nil {
			var udpr []int
			err = json.Unmarshal(b, &udpr)
			if err != nil {
				log.Println("UDPResponse unmarshalling error", err)
				continue
			}
			s.LastHeartbeat = time.Now()
			s.HeldChunks = udpr
			for i := range s.HeldChunks {
				sg.Chunks[i].Holders[s] = true
			}
		} else {
			log.Println("UDP initial request transmission error", err)
		}
	}
	for i := range sg.Chunks {
		sg.Chunks[i].ID = i
	}
	sg.ChunkUpdateChannel = make(chan *VirtualChunk, 1024)
	//Launch UDP heartbeat requester
	heartbeatRequester(sg)
	//All existing chunks must be checked
	for i := range sg.Chunks {
		sg.ChunkUpdateChannel <- &sg.Chunks[i]
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
		log.Println("Server added to", s.Phy)
	}
	//Add to local servergroup instance
	sg.Unlock()
	sg.AddServerToGroup(localhost)
	sg.Lock()
	sg.Localhost = sg.Servers[localhost]
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

			s := l.Front().Value.(*VirtualServer)

			//Request known chunks list
			b, err := tlLowCom.UDPRequest(s.Phy, time.Millisecond*50)
			if err == nil {
				//Unmarshal response
				var udpr []int
				err = json.Unmarshal(b, &udpr)
				if err != nil {
					panic(err)
				}
				//Detect added chunks
				for _, c := range udpr {
					ok := false
					for _, c2 := range s.HeldChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.ChunkUpdateChannel <- &sg.Chunks[c]
					}
				}
				//Detect forgotten chunks
				for _, c := range s.HeldChunks {
					ok := false
					for _, c2 := range udpr {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.ChunkUpdateChannel <- &sg.Chunks[c]
					}
				}

				s.HeldChunks = udpr
				//Remove old chunks
				for c := range s.HeldChunks {
					delete(sg.Chunks[c].Holders, s)
				}
				//Add new chunks
				for c := range s.HeldChunks {
					sg.Chunks[c].Holders[s] = true
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
	log.Println("Server", addr, "added\n", sg)
	return nil
}

func oldServersRemover() {
	for {
		time.Sleep(time.Minute)
		//check for dead servers
		//remove them
	}
}
