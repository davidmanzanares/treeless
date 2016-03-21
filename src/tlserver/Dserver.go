package tlserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"time"
	"treeless/src/tlcom"
	"treeless/src/tlcom/tlproto"
	"treeless/src/tlcom/udp"
	"treeless/src/tlcore"
	"treeless/src/tlheartbeat"
	"treeless/src/tllocals"
	"treeless/src/tlrebalance"
	"treeless/src/tlsg"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type DBServer struct {
	//Core
	m *tlcore.Map //todo rename to core
	//Com
	s *tlcom.Server
	//Distribution
	sg *tlsg.ServerGroup
	//Status
	lh *tllocals.LHStatus
	//Heartbeat
	hb *tlheartbeat.Heartbeater
}

const serverWorkers = 4
const channelUpdateBufferSize = 1024

//Start a Treeless server
func Start(addr string, localIP string, localport int, numChunks, redundancy int, dbpath string) *DBServer {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(localIP + ":" + fmt.Sprint(localport) + " ")
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			log.Println("STACK:\n", string(debug.Stack()))
			panic(r)
		}
	}()
	var s DBServer
	var err error

	//Servergroup initialization
	if addr == "" {
		//Launch core
		s.m = tlcore.NewMap(dbpath, numChunks)
		if err != nil {
			panic(err)
		}
		if addr == "" {
			for i := 0; i < numChunks; i++ {
				s.m.ChunkEnable(i)
			}
		}

		s.lh = tllocals.NewLHStatus(numChunks, localIP+":"+fmt.Sprint(localport))
		for i := 0; i < numChunks; i++ {
			s.lh.ChunkSetStatus(i, tllocals.ChunkSynched)
		}

		//New DB group
		s.sg = tlsg.CreateServerGroup(numChunks, redundancy)
		s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localport))
		list := make([]int, numChunks)
		for i := 0; i < numChunks; i++ {
			list[i] = i
		}
		s.sg.SetServerChunks(localIP+":"+fmt.Sprint(localport), list)
	} else {
		//Associate to an existing DB group
		s.sg, err = tlsg.Assoc(addr)
		if err != nil {
			panic(err)
		}

		numChunks = s.sg.NumChunks()

		//Launch core
		s.m = tlcore.NewMap(dbpath, numChunks)
		if err != nil {
			panic(err)
		}
		if addr == "" {
			for i := 0; i < numChunks; i++ {
				s.m.ChunkEnable(i)
			}
		}

		s.lh = tllocals.NewLHStatus(numChunks, localIP+":"+fmt.Sprint(localport))

		//Add to external servergroup instances
		//For each other server: add localhost
		for _, s2 := range s.sg.Servers() {
			err = s2.AddServerToGroup(s.lh.LocalhostIPPort)
			if err != nil {
				panic(err)
			}
		}
		s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localport))
	}

	//Heartbeat start
	s.hb = tlheartbeat.Start(s.sg)
	//Rebalancer start
	tlrebalance.StartRebalance(s.sg, s.lh, s.m, func() bool { return false })

	//Init server
	readChannel := make(chan tlproto.Message, 1024)
	for i := 0; i < serverWorkers; i++ {
		createWorker(&s, readChannel)
	}
	s.s = tlcom.Start(addr, localIP, localport, readChannel, udpCreateReplier(s.sg, s.lh))
	log.Println("Server boot-up completed")
	return &s
}

//Stop the server, close all TCP/UDP connections
func (s *DBServer) Stop() {
	s.hb.Stop()
	s.s.Stop()
	s.sg.Stop()
	s.m.Close()
}

func (s *DBServer) LogInfo() {
	log.Println("Info log")
	log.Println(s.sg)
}

func udpCreateReplier(sg *tlsg.ServerGroup, lh *tllocals.LHStatus) tlcom.UDPCallback {
	return func() tlUDP.AmAlive {
		var r tlUDP.AmAlive
		r.KnownChunks = lh.KnownChunksList()
		r.KnownServers = sg.KnownServers()
		//log.Println("UDP AA", r)
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
				s.m.Delete(message.Key, message.Value)
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
				if s.lh.ChunkStatus(chunkID) == tllocals.ChunkSynched {
					//New goroutine will put every key value pair into destination, it will manage the OpTransferOK response
					addr := string(message.Value)
					c, err := tlcom.CreateConnection(addr, func() {})
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
				s.sg.AddServerToGroup(string(message.Key))
				response.Type = tlproto.OpAddServerToGroupACK
				responseChannel <- response
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
			case tlproto.OpProtect:
				var response tlproto.Message
				response.ID = message.ID
				chunkID := binary.LittleEndian.Uint32(message.Key)
				if s.lh.ChunkStatus(int(chunkID)) == tllocals.ChunkSynched && s.sg.NumHolders(int(chunkID)) > s.sg.Redundancy() {
					response.Type = tlproto.OpProtectOK
				} else {
					response.Type = tlproto.OpErr
				}
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
