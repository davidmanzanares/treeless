package server

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"treeless/com"
	"treeless/com/protocol"
	"treeless/core"
	"treeless/dist/heartbeat"
	"treeless/dist/rebalance"
	"treeless/dist/repair"
	"treeless/dist/servergroup"
)

//DBServer manages a Treeless node server
type DBServer struct {
	core    *core.Core
	server  *com.Server
	sg      *servergroup.ServerGroup
	hb      *heartbeat.Heartbeater
	stopped uint32
}

//Create creates a new DB server group
//localIP and localPort sets the ip:port to use by this server
//localDBpath sets the path to store/open the DB
//localChunkSize sets the server chunk size in bytes
//openDB should be true if you want to open an already stored DB, set it to false if you want to create a new DB, overwriting previous DB if it exists
//numChunks is the number of chunks to use in the new server group
//redundancy is the level of redundancy to use in the new server group, 1 means that only one server will have each chunk/partition
func Create(localIP string, localPort int, localDBpath string, localChunkSize uint64, openDB bool, numChunks, redundancy int) *DBServer {
	s := new(DBServer)
	//Core
	s.core = core.New(localDBpath, localChunkSize, numChunks)
	if openDB {
		s.core.Open()
	} else {
		for i := 0; i < numChunks; i++ {
			s.core.ChunkSetPresent(i)
		}
	}
	//Servergroup
	s.sg = servergroup.CreateServerGroup(numChunks, redundancy, localIP+":"+fmt.Sprint(localPort))
	s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localPort))
	list := make([]protocol.AmAliveChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		list[i].ID = i
	}
	s.sg.SetServerChunks(localIP+":"+fmt.Sprint(localPort), list)
	//Heartbeat
	s.hb = heartbeat.Start(s.sg)
	//Rebalance
	rebalance.StartRebalance(s.sg, s.core, s.isStopped)
	//Repair
	repair.StartRepairSystem(s.sg, s.core, s.isStopped)
	//Server
	s.server = com.Start(localIP, localPort, s.processMessage, s.hb.ListenReply(s.core))
	log.Println("Server boot-up completed")
	return s
}

//Assoc associates a new DB server node to an existint server group
//localIP and localPort sets the ip:port to use by this server
//localDBpath sets the path to store/open the DB
//localChunkSize sets the server chunk size in bytes
//openDB should be true if you want to open an already stored DB, set it to false if you want to create a new DB, overwriting previous DB if it exists
//assocAddr is the ip:port address of one of the server groups nodes, it will be used at initialization time to associate this server
func Assoc(localIP string, localPort int, localDBpath string, localChunkSize uint64, openDB bool, assocAddr string) *DBServer {
	s := new(DBServer)
	//Associate to an existing DB group
	var err error
	s.sg, err = servergroup.Assoc(assocAddr, localIP+":"+fmt.Sprint(localPort))
	if err != nil {
		panic(err)
	}

	numChunks := s.sg.NumChunks()
	//Launch core
	s.core = core.New(localDBpath, localChunkSize, numChunks)
	if openDB {
		s.core.Open()
	}
	//Add this server to the server group
	addedAtLeastOnce := false
	for _, s2 := range s.sg.Servers() {
		err = s2.AddServerToGroup(s.sg.LocalhostIPPort)
		if err != nil {
			log.Println(err)
		} else {
			addedAtLeastOnce = true
		}
	}
	if !addedAtLeastOnce {
		panic("None add server to group ACK recieved")
	}
	s.sg.AddServerToGroup(localIP + ":" + fmt.Sprint(localPort))
	//Heartbeat
	s.hb = heartbeat.Start(s.sg)
	//Rebalance
	rebalance.StartRebalance(s.sg, s.core, s.isStopped)
	//Repair
	repair.StartRepairSystem(s.sg, s.core, s.isStopped)
	//Server
	s.server = com.Start(localIP, localPort, s.processMessage, s.hb.ListenReply(s.core))
	log.Println("Server boot-up completed")
	return s
}

//Stop the server
func (s *DBServer) Stop() {
	log.Println("Server close initiated")
	atomic.StoreUint32(&s.stopped, 1)
	s.hb.Stop()
	s.server.Stop()
	s.sg.Stop()
	s.core.Close()
	log.Println("Server closed")
}

func (s *DBServer) isStopped() bool {
	return atomic.LoadUint32(&s.stopped) > 0
}

func (s *DBServer) processMessage(message protocol.Message) (response protocol.Message) {
	//fmt.Println("Server", "message received", string(message.Key), string(message.Value), message.Type)
	response.Type = 0
	if s.isStopped() {
		return response
	}
	response.ID = message.ID
	switch message.Type {
	case protocol.OpGet:
		value, _ := s.core.Get(message.Key)
		response.Type = protocol.OpResponse
		response.Value = value
	case protocol.OpSet:
		err := s.core.Set(message.Key, message.Value)
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpAsyncSet:
		s.core.Set(message.Key, message.Value)
	case protocol.OpCAS:
		err := s.core.CAS(message.Key, message.Value, s.sg.IsSynched)
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpDel:
		err := s.core.Delete(message.Key, message.Value)
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpTransfer:
		chunkID := int(binary.LittleEndian.Uint32(message.Key))
		//New goroutine will put every key value pair into destination, it will manage the OpTransferOK response
		go func() {
			addr := string(message.Value)
			c, err := com.CreateConnection(addr, func() {})
			defer c.Close()
			if err != nil {
				log.Println("Transfer failed, error:", err)
			} else {
				i := 0
				s.core.Iterate(chunkID, func(key, value []byte) bool {
					if i%100 == 0 {
						ch := c.Set(key, value, time.Millisecond*500)
						err = ch.Wait()
					} else {
						c.Set(key, value, 0) //AsyncSet
					}
					i++
					return true
				})
				log.Println("Transfer operation completed, pairs:", i)
			}
		}()
		response.Type = protocol.OpOK
	case protocol.OpGetConf:
		b, err := s.sg.Marshal()
		if err != nil {
			panic(err)
		}
		response.Type = protocol.OpResponse
		response.Value = b
	case protocol.OpAddServerToGroup:
		addr := string(message.Key)
		s.sg.AddServerToGroup(addr)
		s.hb.GossipAdded(addr)
		response.Type = protocol.OpOK
	case protocol.OpGetChunkInfo:
		chunkID := int(binary.LittleEndian.Uint32(message.Key))
		response.Type = protocol.OpResponse
		response.Value = make([]byte, 8)
		length := s.core.LengthOfChunk(chunkID)
		binary.LittleEndian.PutUint64(response.Value, length)
	case protocol.OpProtect:
		chunkID := binary.LittleEndian.Uint32(message.Key)
		if s.sg.NumHolders(int(chunkID)) > s.sg.Redundancy() {
			err := s.core.ChunkSetProtected(int(chunkID))
			if err == nil {
				response.Type = protocol.OpOK
			} else {
				response.Type = protocol.OpErr
				response.Value = []byte(err.Error())
			}
		} else {
			response.Type = protocol.OpErr
		}
	case protocol.OpSetBuffered:
		response.Type = protocol.OpSetBuffered
	case protocol.OpSetNoDelay:
		response.Type = protocol.OpSetNoDelay
	default:
		response.Type = protocol.OpErr
		response.Value = []byte("Operation not supported")
		log.Println("Operation not supported", message.Type)
	}
	return response
}
