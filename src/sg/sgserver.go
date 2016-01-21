package tlsg

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"sync/atomic"
	"treeless/src/com"
	"treeless/src/com/tcp"
	"treeless/src/com/udp"
	"treeless/src/core"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	//Core
	m *tlcore.Map
	//Com
	s *tlcom.Server
	//Distribution
	sg *ServerGroup
	//Status
	stopped int32
}

//Start a Treeless server
func Start(addr string, localport string, numChunks, redundancy int, dbpath string) *Server {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(tlcom.GetLocalIP() + ":" + localport + " ")
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()
	var s Server
	//Launch core
	var err error
	s.m = tlcore.NewMap(dbpath, numChunks)
	if err != nil {
		panic(err)
	}
	//Servergroup initialization
	if addr == "" {
		//New DB group
		s.sg = CreateServerGroup(len(s.m.Chunks), localport, redundancy)
	} else {
		//Associate to an existing DB group
		s.sg, err = Associate(addr, localport)
		if err != nil {
			panic(err)
		}
	}
	//Init server
	s.s = tlcom.Start(addr, localport, tcpCreateReplier(&s), udpCreateReplier(s.sg))
	//Rebalancer
	//TODO order is good??? rebalance-server
	Rebalance(s.sg)
	log.Println("Server boot-up completed")
	return &s
}

//IsStopped returns true if the server is not running
func (s *Server) IsStopped() bool {
	return atomic.LoadInt32(&s.stopped) != 0
}

//Stop the server, close all TCP/UDP connections
func (s *Server) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.s.Stop()
	s.m.Close()
	s.sg.Stop()
}

func (s *Server) LogInfo() {
	log.Println("Info log")
	log.Println(s.sg)
}

func udpCreateReplier(sg *ServerGroup) tlcom.UDPCallback {
	return func() tlUDP.AmAlive {
		var r tlUDP.AmAlive
		for i := 0; i < sg.NumChunks; i++ {
			if sg.IsChunkPresent(i) {
				r.KnownChunks = append(r.KnownChunks, i)
			}
		}
		for _, s := range sg.Servers {
			r.KnownServers = append(r.KnownServers, s.Phy)
		}
		return r
	}
}

func tcpCreateReplier(s *Server) tlcom.TCPCallback {
	return func(message tlTCP.Message, responseChannel chan tlTCP.Message) {
		//fmt.Println("Server", conn.LocalAddr(), "message recieved", string(message.Key), string(message.Value))
		switch message.Type {
		case tlcore.OpGet:
			var response tlTCP.Message
			rval, err := s.m.Get(message.Key)
			//fmt.Println("Get operation", key, rval, err)
			response.ID = message.ID
			if err != nil {
				response.Type = tlTCP.OpErr
				response.Value = []byte(err.Error())
			} else {
				response.Type = tlTCP.OpGetResponse
				response.Value = rval
			}
			responseChannel <- response
		case tlcore.OpSet:
			s.m.Set(message.Key, message.Value)
			//TODO err response
			//fmt.Println("Put operation", message.Key, message.Value, err)
			//if err != nil {
			//	panic(err)
			//}
		case tlTCP.OpTransfer:
			var chunkID int
			err := json.Unmarshal(message.Key, &chunkID)
			if err != nil {
				panic(string(message.Key) + err.Error())
			}
			transferFail := func() {
				var response tlTCP.Message
				response.ID = message.ID
				response.Type = tlTCP.OpErr
				responseChannel <- response
			}
			if s.sg.IsChunkPresent(chunkID) {
				//New goroutine will put every key value pair into destination, it will manage the OpTransferOK response
				addr := string(message.Value)
				c, err := tlcom.CreateConnection(addr)
				if err != nil {
					log.Println(1111111111, err)
					transferFail()
				} else {
					go func(c *tlcom.Conn) {
						s.m.Iterate(chunkID, func(key, value []byte) {
							c.Set(key, value)
						})
						c.GetAccessInfo()
						c.Close()
						var response tlTCP.Message
						response.ID = message.ID
						response.Type = tlTCP.OpTransferCompleted
						responseChannel <- response
					}(c)
				}
			} else {
				transferFail()
			}
		case tlTCP.OpGetConf:
			var response tlTCP.Message
			response.ID = message.ID
			response.Type = tlTCP.OpGetConfResponse
			b, err := s.sg.Marshal()
			if err != nil {
				panic(err)
			}
			response.Value = b
			responseChannel <- response
		case tlTCP.OpAddServerToGroup:
			var response tlTCP.Message
			response.ID = message.ID
			err := s.sg.AddServerToGroup(string(message.Key))
			if err != nil {
				response.Type = tlTCP.OpErr
				response.Value = []byte(err.Error())
				responseChannel <- response
			} else {
				response.Type = tlTCP.OpAddServerToGroupACK
				responseChannel <- response
			}
		case tlTCP.OpGetChunkInfo:
			var response tlTCP.Message
			response.ID = message.ID
			c := s.m.Chunks[binary.LittleEndian.Uint32(message.Key)]
			response.Type = tlTCP.OpGetChunkInfoResponse
			response.Value = make([]byte, 8)
			binary.LittleEndian.PutUint64(response.Value, c.St.Length+1)
			responseChannel <- response
		default:
			var response tlTCP.Message
			response.ID = message.ID
			response.Type = tlTCP.OpErr
			response.Value = []byte("Operation not supported")
			responseChannel <- response
		}
	}
}
