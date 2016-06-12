package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"treeless/com"
	"treeless/com/protocol"
	"treeless/dist/heartbeat"
	"treeless/dist/rebalance"
	"treeless/dist/repair"
	"treeless/dist/servergroup"
	"treeless/local"
)

//DBServer manages a Treeless node server
type DBServer struct {
	core    *local.Core
	server  *tlcom.Server
	sg      *servergroup.ServerGroup
	hb      *heartbeat.Heartbeater
	stopped uint32
}

func Create(localIP string, localPort int, localDBpath string, localChunkSize uint64, openDB bool, numChunks, redundancy int) *DBServer {
	s := new(DBServer)
	//Core
	s.core = local.NewCore(localDBpath, localChunkSize, numChunks, localIP+":"+fmt.Sprint(localPort))
	if openDB {
		s.core.Open()
	} else {
		for i := 0; i < numChunks; i++ {
			s.core.ChunkSetSynched(i)
		}
	}
	//Servergroup
	s.sg = servergroup.CreateServerGroup(numChunks, redundancy)
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
	s.server = tlcom.Start(localIP, localPort, s.processMessage, s.hb.ListenReply(s.core))
	log.Println("Server boot-up completed")
	return s
}

func Assoc(localIP string, localPort int, localDBpath string, localChunkSize uint64, openDB bool, assocAddr string) *DBServer {
	s := new(DBServer)
	//Associate to an existing DB group
	var err error
	s.sg, err = servergroup.Assoc(assocAddr)
	if err != nil {
		panic(err)
	}

	numChunks := s.sg.NumChunks()
	//Launch core
	s.core = local.NewCore(localDBpath, localChunkSize, numChunks, localIP+":"+fmt.Sprint(localPort))
	if openDB {
		s.core.Open()
	}
	//Add this server to the server group
	addedAtLeastOnce := false
	for _, s2 := range s.sg.Servers() {
		err = s2.AddServerToGroup(s.core.LocalhostIPPort)
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
	s.server = tlcom.Start(localIP, localPort, s.processMessage, s.hb.ListenReply(s.core))
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
	switch message.Type {
	case protocol.OpGet:
		value, _ := s.core.Get(message.Key)
		response.ID = message.ID
		response.Type = protocol.OpResponse
		response.Value = value
	case protocol.OpSet:
		err := s.core.Set(message.Key, message.Value)
		response.ID = message.ID
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpAsyncSet:
		s.core.Set(message.Key, message.Value)
	case protocol.OpCAS:
		err := s.core.CAS(message.Key, message.Value)
		response.ID = message.ID
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpDel:
		err := s.core.Delete(message.Key, message.Value)
		response.ID = message.ID
		if err == nil {
			response.Type = protocol.OpOK
		} else {
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
		}
	case protocol.OpTransfer:
		transferFail := func(err error) protocol.Message { //TODO up
			var response protocol.Message
			response.ID = message.ID
			response.Type = protocol.OpErr
			response.Value = []byte(err.Error())
			return response
		}
		var chunkID int
		err := json.Unmarshal(message.Key, &chunkID) //TODO use uint32
		if err != nil {
			return transferFail(err)
		}
		//New goroutine will put every key value pair into destination, it will manage the OpTransferOK response
		go func() {
			addr := string(message.Value)
			c, err := tlcom.CreateConnection(addr, func() {})
			defer c.Close()
			if err != nil {
				log.Println("Transfer failed, error:", err)
			} else {
				i := 0
				s.core.BackwardsIterate(chunkID, func(key, value []byte) bool {
					ch := c.Set(key, value, time.Millisecond*500)
					err = ch.Wait()
					i++
					return true
				})
				log.Println("Transfer operation completed, pairs:", i)
			}
		}()
		response.ID = message.ID
		response.Type = protocol.OpOK
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
		length := s.core.LengthOfChunk(chunkID)
		binary.LittleEndian.PutUint64(response.Value, length)
		return response
	case protocol.OpProtect:
		var response protocol.Message
		response.ID = message.ID
		chunkID := binary.LittleEndian.Uint32(message.Key)
		if s.core.ChunkStatus(int(chunkID)) == local.ChunkSynched && s.sg.NumHolders(int(chunkID)) > s.sg.Redundancy() {
			s.core.ChunkSetProtected(int(chunkID))
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
