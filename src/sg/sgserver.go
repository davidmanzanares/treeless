package tlsg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"treeless/src/com"
	"treeless/src/com/tcp"
	"treeless/src/com/udp"
	"treeless/src/core"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type DBServer struct {
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
func Start(addr string, localport int, numChunks, redundancy int, dbpath string) *DBServer {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(tlcom.GetLocalIP() + ":" + fmt.Sprint(localport) + " ")
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()
	var s DBServer
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
	log.Println("Server boot-up completed")
	return &s
}

//IsStopped returns true if the server is not running
func (s *DBServer) IsStopped() bool {
	return atomic.LoadInt32(&s.stopped) != 0
}

//Stop the server, close all TCP/UDP connections
func (s *DBServer) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.s.Stop()
	s.m.Close()
	s.sg.Stop()
}

func (s *DBServer) LogInfo() {
	log.Println("Info log")
	log.Println(s.sg)
}

func udpCreateReplier(sg *ServerGroup) tlcom.UDPCallback {
	return func() tlUDP.AmAlive {
		var r tlUDP.AmAlive
		sg.Lock()
		defer sg.Unlock()
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

func tcpCreateReplier(s *DBServer) tlcom.TCPCallback {
	return func(responseChannel chan<- tlTCP.Message, read <-chan tlTCP.Message) {
		for { //TODO refactor make fixed number of workers
			message, ok := <-read
			if !ok {
				close(responseChannel)
				return
			}
			//fmt.Println("Server", conn.LocalAddr(), "message recieved", string(message.Key), string(message.Value))
			switch message.Type {
			case tlcore.OpGet:
				var response tlTCP.Message
				rval, err := s.m.Get(message.Key)
				//cid := tlhash.GetChunkID(message.Key, s.sg.NumChunks)
				/*if s.sg.ChunkStatus(cid) != ChunkSynched {
					fmt.Println("ASD")
				}*/
				//fmt.Println(s.sg.ChunkStatus(cid), cid)
				//fmt.Println("Get operation", message.Key, rval, err)
				response.ID = message.ID
				if err != nil {
					response.Type = tlTCP.OpGetResponse
				} else {
					response.Type = tlTCP.OpGetResponse
					response.Value = rval
				}
				responseChannel <- response
			case tlcore.OpSet:
				var response tlTCP.Message
				if len(message.Value) < 8 {
					log.Println("Error: message value len < 8")
					return
				}
				err := s.m.Set(message.Key, message.Value)
				response.ID = message.ID
				if err == nil {
					response.Type = tlTCP.OpSetOK
				} else {
					response.Type = tlTCP.OpErr
					response.Value = []byte(err.Error())
				}
				responseChannel <- response
			case tlcore.OpDel:
				var response tlTCP.Message
				err := s.m.Delete(message.Key)
				response.ID = message.ID
				if err == nil {
					response.Type = tlTCP.OpDelOK
				} else {
					response.Type = tlTCP.OpErr
					response.Value = []byte(err.Error())
				}
				responseChannel <- response
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
						log.Println("Transfer failed, error:", err)
						transferFail()
					} else {
						go func(c *tlcom.Conn) {
							i := 0
							s.m.Iterate(chunkID, func(key, value []byte) {
								err := c.Set(key, value)
								if err != nil {
									//TODO transfer aborted
									log.Println("Transfer error:", err)
									panic(err)
								}
								i++
								/*if i%1024 == 0 {
									log.Println("Transfered ", i, "keys")
								}*/
								//fmt.Println("Transfer operation", key, value)
							})
							fmt.Println("Transfer operation completed, pairs:", i)
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
				c.Lock()
				binary.LittleEndian.PutUint64(response.Value, c.St.Length+1)
				c.Unlock()
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
}
