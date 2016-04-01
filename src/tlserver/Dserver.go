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
	"treeless/src/tlhash"
	"treeless/src/tlheartbeat"
	"treeless/src/tllocals"
	"treeless/src/tlrebalance"
	"treeless/src/tlsg"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type DBServer struct {
	//Core
	lh *tllocals.LHStatus
	//Com
	s *tlcom.Server
	//Distribution
	sg *tlsg.ServerGroup
	//Heartbeat
	hb *tlheartbeat.Heartbeater
}

const serverWorkers = 4
const channelUpdateBufferSize = 1024

//Start a Treeless server
func Start(addr string, localIP string, localport int, numChunks, redundancy int, dbpath string, size uint64) *DBServer {
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
		s.lh = tllocals.NewLHStatus(dbpath, size, numChunks, localIP+":"+fmt.Sprint(localport))
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
		s.lh = tllocals.NewLHStatus(dbpath, size, numChunks, localIP+":"+fmt.Sprint(localport))

		//Add to external servergroup instances
		//For each other server: add localhost
		addedAtLeastOnce := false
		for _, s2 := range s.sg.Servers() {
			err = s2.AddServerToGroup(s.lh.LocalhostIPPort)
			if err != nil {
				log.Println(err)
			} else {
				addedAtLeastOnce = true
			}
		}
		if !addedAtLeastOnce {
			panic("None add server to group ACK recieved")
		}
		s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localport))
	}

	//Heartbeat start
	s.hb = tlheartbeat.Start(s.sg)
	//Rebalancer start
	tlrebalance.StartRebalance(s.sg, s.lh, func() bool { return false })

	//Init server
	s.s = tlcom.Start(addr, localIP, localport, worker(&s), udpCreateReplier(s.sg, s.lh))
	log.Println("Server boot-up completed")
	return &s
}

//Stop the server, close all TCP/UDP connections
func (s *DBServer) Stop() {
	s.hb.Stop()
	s.s.Stop()
	s.sg.Stop()
	s.lh.Close()
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

func worker(s *DBServer) (work func(message tlproto.Message) (response tlproto.Message)) {
	return func(message tlproto.Message) (response tlproto.Message) {
		//fmt.Println("Server", "message recieved", string(message.Key), string(message.Value), message.Type)
		switch message.Type {
		case tlproto.OpGet:
			rval, _ := s.lh.Get(message.Key)
			//cid := tlhash.GetChunkID(message.Key, s.sg.NumChunks)
			/*if s.sg.ChunkStatus(cid) != ChunkSynched {
				fmt.Println("ASD")
			}*/
			//fmt.Println(s.sg.ChunkStatus(cid), cid)
			//fmt.Println("Get operation", message.Key, rval, err)
			response.ID = message.ID
			response.Type = tlproto.OpGetResponse
			response.Value = rval
			return response
		case tlproto.OpSet:
			var response tlproto.Message

			if len(message.Value) < 8 {
				log.Println("Error: message value len < 8")
				return response
			}
			err := s.lh.Set(message.Key, message.Value)
			response.ID = message.ID
			if err == nil {
				response.Type = tlproto.OpSetOK
			} else {
				response.Type = tlproto.OpErr
				response.Value = []byte(err.Error())
			}
			return response
		case tlproto.OpCAS:
			var response tlproto.Message
			if len(message.Value) < 24 {
				log.Println("Error: CAS value len < 16")
				return response
			}
			h := tlhash.FNV1a64(message.Key)
			chunkIndex := int((h >> 32) % uint64(s.sg.NumChunks()))
			if s.lh.ChunkStatus(chunkIndex) != tllocals.ChunkSynched {
				response.ID = message.ID
				response.Key = make([]byte, 1)
				response.Key[0] = 1
				response.Type = tlproto.OpErr
				response.Value = []byte("Not Synched")
				return response
			}
			err := s.lh.CAS(message.Key, message.Value)
			response.ID = message.ID
			response.Key = make([]byte, 1)
			if err == nil {
				response.Type = tlproto.OpCASOK
			} else {
				response.Type = tlproto.OpErr
				response.Value = []byte(err.Error())
			}
			return response
		case tlproto.OpDel:
			var response tlproto.Message
			s.lh.Delete(message.Key, message.Value)
			response.ID = message.ID
			response.Type = tlproto.OpDelOK
			return response
		case tlproto.OpTransfer:
			var chunkID int
			err := json.Unmarshal(message.Key, &chunkID)
			if err != nil {
				panic(string(message.Key) + err.Error())
			}
			transferFail := func() tlproto.Message {
				var response tlproto.Message
				response.ID = message.ID
				response.Type = tlproto.OpErr
				return response
			}
			if s.lh.ChunkStatus(chunkID) == tllocals.ChunkSynched {
				//New goroutine will put every key value pair into destination, it will manage the OpTransferOK response
				addr := string(message.Value)
				c, err := tlcom.CreateConnection(addr, func() {})
				if err != nil {
					log.Println("Transfer failed, error:", err)
					transferFail()
				} else {
					i := 0
					var err error
					s.lh.Iterate(chunkID, func(key, value []byte) bool {
						ch := c.Set(key, value, time.Millisecond*500)
						err = ch.Wait()
						if err != nil {
							return false
						}
						i++
						return true
						/*if i%1024 == 0 {
							log.Println("Transfered ", i, "keys")
						}*/
						//fmt.Println("Transfer operation", key, value)
					})
					if err == nil {
						fmt.Println("Transfer operation completed, pairs:", i)
						c.Close()
						var response tlproto.Message
						response.ID = message.ID
						response.Type = tlproto.OpTransferCompleted
						return response
					}
					//TODO transfer aborted
					log.Println("Transfer error:", err)
					var response tlproto.Message
					response.ID = message.ID
					response.Type = tlproto.OpErr
					response.Value = []byte(err.Error())
					c.Close()
					return response
				}
			} else {
				return transferFail()
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
			return response
		case tlproto.OpAddServerToGroup:
			var response tlproto.Message
			response.ID = message.ID
			s.sg.AddServerToGroup(string(message.Key))
			response.Type = tlproto.OpAddServerToGroupACK
			return response
		case tlproto.OpGetChunkInfo:
			var response tlproto.Message
			response.ID = message.ID
			chunkID := int(binary.LittleEndian.Uint32(message.Key))
			response.Type = tlproto.OpGetChunkInfoResponse
			response.Value = make([]byte, 8)
			length := s.lh.LengthOfChunk(chunkID)
			binary.LittleEndian.PutUint64(response.Value, length)
			return response
		case tlproto.OpProtect:
			var response tlproto.Message
			response.ID = message.ID
			chunkID := binary.LittleEndian.Uint32(message.Key)
			if s.lh.ChunkStatus(int(chunkID)) == tllocals.ChunkSynched && s.sg.NumHolders(int(chunkID)) > s.sg.Redundancy() {
				response.Type = tlproto.OpProtectOK
			} else {
				response.Type = tlproto.OpErr
			}
			return response
		default:
			var response tlproto.Message
			response.ID = message.ID
			response.Type = tlproto.OpErr
			response.Value = []byte("Operation not supported")
			return response
		}
		return response
	}
}
