package tlsg

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"
	"treeless/src/com"
	"treeless/src/com/tlproto"
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

const serverWorkers = 4

//Start a Treeless server
func Start(addr string, localIP string, localport int, numChunks, redundancy int, dbpath string) *DBServer {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(localIP + ":" + fmt.Sprint(localport) + " ")
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r, "STACK:", string(debug.Stack()))
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
		s.sg = CreateServerGroup(len(s.m.Chunks), localIP, localport, redundancy)
	} else {
		//Associate to an existing DB group
		s.sg, err = Associate(addr, localIP, localport)
		if err != nil {
			panic(err)
		}
	}
	//Init server
	readChannel := make(chan tlproto.Message, 1024)
	for i := 0; i < serverWorkers; i++ {
		createWorker(&s, readChannel)
	}
	s.s = tlcom.Start(addr, localIP, localport, readChannel, udpCreateReplier(s.sg))
	log.Println("Server boot-up completed")
	return &s
}

//IsStopped returns true if the server is not running
func (s *DBServer) IsStopped() bool {
	return atomic.LoadInt32(&s.stopped) != 0
}

//Stop the server, close all TCP/UDP connections
func (s *DBServer) Stop() {
	atomic.StoreInt32(&s.stopped, 1) //TODO reorder
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

func createWorker(s *DBServer, readChannel <-chan tlproto.Message) {
	go func() {
		for { //TODO refactor make fixed number of workers
			message, ok := <-readChannel
			responseChannel := message.ResponseChannel
			if !ok {
				close(responseChannel)
				return
			}
			//fmt.Println("Server", conn.LocalAddr(), "message recieved", string(message.Key), string(message.Value))
			switch message.Type {
			case tlproto.OpGet:
				var response tlproto.Message
				rval, _ := s.m.Get(message.Key)
				//cid := tlhash.GetChunkID(message.Key, s.sg.NumChunks)
				/*if s.sg.ChunkStatus(cid) != ChunkSynched {
					fmt.Println("ASD")
				}*/
				//fmt.Println(s.sg.ChunkStatus(cid), cid)
				//fmt.Println("Get operation", message.Key, rval, err)
				response.ID = message.ID
				response.Type = tlproto.OpGetResponse
				response.Value = rval
				responseChannel <- response
			case tlproto.OpSet:
				var response tlproto.Message
				if len(message.Value) < 8 {
					log.Println("Error: message value len < 8")
					return
				}
				err := s.m.Set(message.Key, message.Value)
				response.ID = message.ID
				if err == nil {
					response.Type = tlproto.OpSetOK
				} else {
					response.Type = tlproto.OpErr
					response.Value = []byte(err.Error())
				}
				responseChannel <- response
			case tlproto.OpDel:
				var response tlproto.Message
				s.m.Delete(message.Key)
				response.ID = message.ID
				response.Type = tlproto.OpDelOK
				responseChannel <- response
			case tlproto.OpTransfer:
				var chunkID int
				err := json.Unmarshal(message.Key, &chunkID)
				if err != nil {
					panic(string(message.Key) + err.Error())
				}
				transferFail := func() {
					var response tlproto.Message
					response.ID = message.ID
					response.Type = tlproto.OpErr
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
								err := c.Set(key, value, time.Millisecond*100)
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
							var response tlproto.Message
							response.ID = message.ID
							response.Type = tlproto.OpTransferCompleted
							responseChannel <- response
						}(c)
					}
				} else {
					transferFail()
				}
			case tlproto.OpGetConf:
				var response tlproto.Message
				response.ID = message.ID
				response.Type = tlproto.OpGetConfResponse
				b, err := s.sg.Marshal()
				if err != nil {
					panic(err)
				}
				response.Value = b
				responseChannel <- response
			case tlproto.OpAddServerToGroup:
				var response tlproto.Message
				response.ID = message.ID
				err := s.sg.AddServerToGroup(string(message.Key))
				if err != nil {
					response.Type = tlproto.OpErr
					response.Value = []byte(err.Error())
					responseChannel <- response
				} else {
					response.Type = tlproto.OpAddServerToGroupACK
					responseChannel <- response
				}
			case tlproto.OpGetChunkInfo:
				var response tlproto.Message
				response.ID = message.ID
				c := s.m.Chunks[binary.LittleEndian.Uint32(message.Key)]
				response.Type = tlproto.OpGetChunkInfoResponse
				response.Value = make([]byte, 8)
				c.Lock()
				binary.LittleEndian.PutUint64(response.Value, c.St.Length+1)
				c.Unlock()
				responseChannel <- response
			default:
				var response tlproto.Message
				response.ID = message.ID
				response.Type = tlproto.OpErr
				response.Value = []byte("Operation not supported")
				responseChannel <- response
			}
		}
	}()
}
