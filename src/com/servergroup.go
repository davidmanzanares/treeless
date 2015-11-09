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
	Redundancy int                       //DB target redundancy
	Servers    map[string]*VirtualServer //Set of all DB servers
	Chunks     []VirtualChunk            //Array of all DB chunks
	//Local things
	localhost     *VirtualServer
	mutex         sync.Mutex //All ServerGroup read/writes are mutex-protected
	heartbeatList *list.List //List of pending chunk reviews
	chunkUpdate   chan *VirtualChunk
}

//String returns a human-readable representation of the server group
func (sg *ServerGroup) String() string {
	str := fmt.Sprint(len(sg.Servers)) + " servers:\n"
	t := time.Now()
	for _, s := range sg.Servers {
		str += "\t Address: " + s.Phy +
			"\n\t\tKnown chunks: " + fmt.Sprint(s.heldChunks) + " Last heartbeat: " + (t.Sub(s.lastHeartbeat)).String() + "\n"
	}
	str += fmt.Sprint(len(sg.Chunks)) + " chunks:\n"
	for i := range sg.Chunks {
		srv := ""
		for k := range sg.Chunks[i].holders {
			srv += "\n\t\t" + k.Phy
		}
		str += "\tChunk " + fmt.Sprint(i) + srv + "\n"
	}
	return str
}

//Marshal number of chunks and servers physical address
func (sg *ServerGroup) Marshal() ([]byte, error) {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	return json.Marshal(sg)
}

//Unmarshal number of chunks and servers physical address
func (sg *ServerGroup) Unmarshal(b []byte) error {
	err := json.Unmarshal(b, sg)
	if err != nil {
		return err
	}
	for i := range sg.Chunks {
		sg.Chunks[i].holders = make(map[*VirtualServer]bool)
	}
	return nil
}

//CreateServerGroup creates a new DB server group, without connecting to an existing group
func CreateServerGroup(numChunks int, port string, redundancy int) *ServerGroup {
	sg := new(ServerGroup)
	sg.Redundancy = redundancy
	sg.Servers = make(map[string]*VirtualServer)
	localhost := getLocalIP() + ":" + port
	s := new(VirtualServer)
	sg.localhost = s
	s.lastHeartbeat = time.Now()
	sg.Servers[localhost] = s
	s.Phy = localhost
	s.heldChunks = make([]int, numChunks)
	sg.Chunks = make([]VirtualChunk, numChunks)
	//Add this server
	for i := 0; i < numChunks; i++ {
		sg.Servers[localhost].heldChunks[i] = i
		sg.Chunks[i].id = i
		sg.Chunks[i].holders = make(map[*VirtualServer]bool, 1)
		sg.Chunks[i].holders[sg.Servers[localhost]] = true
	}
	sg.chunkUpdate = make(chan *VirtualChunk, 1024)
	rebalancer(sg)
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
			s.lastHeartbeat = time.Now()
			s.heldChunks = udpr
			for i := range s.heldChunks {
				sg.Chunks[i].holders[s] = true
			}
		} else {
			log.Println("UDP initial request transmission error", err)
		}
	}
	for i := range sg.Chunks {
		sg.Chunks[i].id = i
	}
	sg.chunkUpdate = make(chan *VirtualChunk, 1024)
	//Launch UDP heartbeat requester
	heartbeatRequester(sg)
	//All existing chunks must be checked
	for i := range sg.Chunks {
		sg.chunkUpdate <- &sg.Chunks[i]
	}
	return sg, nil
}

//ConnectAsServer connects to an existing server group and adds this server to it
func ConnectAsServer(destAddr string, localport string) (*ServerGroup, error) {
	//Create a client connection first
	sg, err := ConnectAsClient(destAddr)
	if err != nil {
		panic(err)
	}
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	localhost := getLocalIP() + ":" + localport
	//Add to external servergroup instances
	//For each other server: add localhost
	for _, s := range sg.Servers {
		err := s.NeedConnection()
		if err != nil {
			log.Println("TCP transmission error when trying to add server", err)
			continue
		}
		err = s.conn.AddServerToGroup(localhost)
		if err != nil {
			panic(err)
		}
		log.Println("Server added to", s.Phy)
	}
	//Add to local servergroup instance
	sg.mutex.Unlock()
	sg.addServerToGroup(localhost)
	sg.mutex.Lock()
	sg.localhost = sg.Servers[localhost]
	rebalancer(sg)
	return sg, nil
}

func heartbeatRequester(sg *ServerGroup) {
	d := time.Second * 1
	l := list.New()
	sg.heartbeatList = l
	for _, s := range sg.Servers {
		l.PushFront(s)
	}
	sg.mutex.Lock()
	go func() {
		for {
			sg.mutex.Unlock()
			time.Sleep(d)
			sg.mutex.Lock()

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
					for _, c2 := range s.heldChunks {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.chunkUpdate <- &sg.Chunks[c]
					}
				}
				//Detect forgotten chunks
				for _, c := range s.heldChunks {
					ok := false
					for _, c2 := range udpr {
						if c == c2 {
							ok = true
							break
						}
					}
					if !ok {
						sg.chunkUpdate <- &sg.Chunks[c]
					}
				}

				//Remove old chunks
				for c := range s.heldChunks {
					delete(sg.Chunks[c].holders, s)
				}
				s.heldChunks = udpr
				//Add new chunks
				for c := range s.heldChunks {
					sg.Chunks[c].holders[s] = true
				}
				s.lastHeartbeat = time.Now()
			} else {
				log.Println("UDP request timeout. Server", s.Phy, "UDP error:", err)
			}
			//log.Println(sg)
			l.MoveToBack(l.Front())
		}
	}()
}

func (sg *ServerGroup) addServerToGroup(addr string) error {
	sg.mutex.Lock()
	defer sg.mutex.Unlock()
	s, ok := sg.Servers[addr]
	if !ok {
		s = new(VirtualServer)
		sg.Servers[addr] = s
		s.Phy = addr
		s.lastHeartbeat = time.Now()
		sg.heartbeatList.PushBack(s)
	} else {
		s.lastHeartbeat = time.Now()
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
