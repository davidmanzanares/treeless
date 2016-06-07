package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"time"
	"treeless/com"
	"treeless/com/protocol"
	"treeless/dist/heartbeat"
	"treeless/dist/rebalance"
	"treeless/dist/repair"
	"treeless/dist/servergroup"
	"treeless/hashing"
	"treeless/local"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type DBServer struct {
	//Core
	lh *local.Core
	//Com
	s *tlcom.Server
	//Distribution
	sg *servergroup.ServerGroup
	//Heartbeat
	hb *heartbeat.Heartbeater

	stopped bool
}

const serverWorkers = 4
const channelUpdateBufferSize = 1024

//Start a Treeless server
func Start(addr string, localIP string, localport int, numChunks, redundancy int, dbpath string, size uint64, open bool) *DBServer {
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
		s.lh = local.NewCore(dbpath, size, numChunks, localIP+":"+fmt.Sprint(localport))
		if open {
			s.lh.Open()
		} else {
			for i := 0; i < numChunks; i++ {
				s.lh.ChunkSetSynched(i)
			}
		}

		//New DB group
		s.sg = servergroup.CreateServerGroup(numChunks, redundancy)
		s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localport))
		list := make([]protocol.AmAliveChunk, numChunks)
		for i := 0; i < numChunks; i++ {
			list[i].ID = i
		}
		s.sg.SetServerChunks(localIP+":"+fmt.Sprint(localport), list)
	} else {
		//Associate to an existing DB group
		s.sg, err = servergroup.Assoc(addr)
		if err != nil {
			panic(err)
		}

		numChunks = s.sg.NumChunks()

		//Launch core
		s.lh = local.NewCore(dbpath, size, numChunks, localIP+":"+fmt.Sprint(localport))
		if open {
			s.lh.Open()
		}

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
	s.hb = heartbeat.Start(s.sg)
	//Rebalancer start
	rebalance.StartRebalance(s.sg, s.lh, func() bool { return s.stopped })

	repair.StartRepairSystem(s.sg, s.lh, func() bool { return s.stopped })

	//Init server
	s.s = tlcom.Start(addr, localIP, localport, worker(&s), s.hb.ListenReply(s.lh))
	log.Println("Server boot-up completed")
	return &s
}

//Stop the server, close all TCP/UDP connections
func (s *DBServer) Stop() {
	log.Println("Server close initiated")
	s.stopped = true
	s.hb.Stop()
	s.s.Stop()
	s.sg.Stop()
	s.lh.Close()
	log.Println("Server closed")
}

func (s *DBServer) LogInfo() {
	log.Println("Info log")
	log.Println(s.sg)
}

func worker(s *DBServer) (work func(message protocol.Message) (response protocol.Message)) {
	return func(message protocol.Message) (response protocol.Message) {
		//fmt.Println("Server", "message recieved", string(message.Key), string(message.Value), message.Type)
		response.Type = 0
		if s.stopped {
			return response
		}
		switch message.Type {
		case protocol.OpGet:
			rval, _ := s.lh.Get(message.Key)
			//cid := tlhash.GetChunkID(message.Key, s.sg.NumChunks)
			/*if s.sg.ChunkStatus(cid) != ChunkSynched {
				fmt.Println("ASD")
			}*/
			//fmt.Println(s.sg.ChunkStatus(cid), cid)
			//fmt.Println("Get operation", message.Key, rval, err)
			response.ID = message.ID
			response.Type = protocol.OpResponse
			response.Value = rval
			return response
		case protocol.OpSet:
			var response protocol.Message

			if len(message.Value) < 8 {
				log.Println("Error: message value len < 8")
				return response
			}
			err := s.lh.Set(message.Key, message.Value)
			response.ID = message.ID
			if err == nil {
				response.Type = protocol.OpOK
			} else {
				response.Type = protocol.OpErr
				response.Value = []byte(err.Error())
			}
			return response
		case protocol.OpAsyncSet:
			if len(message.Value) < 8 {
				log.Println("Error: message value len < 8")
			}
			s.lh.Set(message.Key, message.Value)
		case protocol.OpCAS:
			var response protocol.Message
			if len(message.Value) < 24 {
				log.Println("Error: CAS value len < 16")
				return response
			}
			h := hashing.FNV1a64(message.Key)
			chunkIndex := int((h >> 32) % uint64(s.sg.NumChunks()))
			if s.lh.ChunkStatus(chunkIndex) != local.ChunkSynched {
				response.ID = message.ID
				response.Key = make([]byte, 1)
				response.Key[0] = 1
				response.Type = protocol.OpErr
				response.Value = []byte("Not Synched")
				return response
			}
			err := s.lh.CAS(message.Key, message.Value)
			response.ID = message.ID
			response.Key = make([]byte, 1)
			if err == nil {
				response.Type = protocol.OpOK
			} else {
				response.Type = protocol.OpErr
				response.Value = []byte(err.Error())
			}
			return response
		case protocol.OpDel:
			var response protocol.Message
			s.lh.Delete(message.Key, message.Value)
			response.ID = message.ID
			response.Type = protocol.OpOK
			return response
		case protocol.OpTransfer:
			var chunkID int
			err := json.Unmarshal(message.Key, &chunkID)
			if err != nil {
				panic(string(message.Key) + err.Error())
			}
			transferFail := func() protocol.Message {
				var response protocol.Message
				response.ID = message.ID
				response.Type = protocol.OpErr
				return response
			}
			if s.lh.ChunkStatus(chunkID) == local.ChunkSynched {
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
						var response protocol.Message
						response.ID = message.ID
						response.Type = protocol.OpOK
						return response
					}
					//TODO transfer aborted
					log.Println("Transfer error:", err)
					var response protocol.Message
					response.ID = message.ID
					response.Type = protocol.OpErr
					response.Value = []byte(err.Error())
					c.Close()
					return response
				}
			} else {
				return transferFail()
			}
		case protocol.OpGetConf:
			var response protocol.Message
			response.ID = message.ID
			response.Type = protocol.OpResponse
			b, err := s.sg.Marshal()
			if err != nil {
				panic(err)
			}
			response.Value = b
			return response
		case protocol.OpAddServerToGroup:
			var response protocol.Message
			response.ID = message.ID
			addr := string(message.Key)
			s.sg.AddServerToGroup(addr)
			s.hb.GossipAdded(addr)
			response.Type = protocol.OpOK
			return response
		case protocol.OpGetChunkInfo:
			var response protocol.Message
			response.ID = message.ID
			chunkID := int(binary.LittleEndian.Uint32(message.Key))
			response.Type = protocol.OpResponse
			response.Value = make([]byte, 8)
			length := s.lh.LengthOfChunk(chunkID)
			binary.LittleEndian.PutUint64(response.Value, length)
			return response
		case protocol.OpProtect:
			var response protocol.Message
			response.ID = message.ID
			chunkID := binary.LittleEndian.Uint32(message.Key)
			if s.lh.ChunkStatus(int(chunkID)) == local.ChunkSynched && s.sg.NumHolders(int(chunkID)) > s.sg.Redundancy() {
				s.lh.ChunkSetProtected(int(chunkID))
				response.Type = protocol.OpOK
			} else {
				response.Type = protocol.OpErr
				//fmt.Println(s.lh.ChunkStatus(int(chunkID)), s.sg.NumHolders(int(chunkID)), s.sg.Redundancy(), s.sg.String())
			}
			return response
		case protocol.OpSetBuffered:
			response.ID = message.ID
			response.Type = protocol.OpSetBuffered
		case protocol.OpSetNoDelay:
			response.ID = message.ID
			response.Type = protocol.OpSetNoDelay
		default:
			var response protocol.Message
			response.ID = message.ID
			response.Type = protocol.OpErr
			response.Value = []byte("Operation not supported")
			log.Println("Operation not supported", message.Type)
			return response
		}
		return response
	}
}
