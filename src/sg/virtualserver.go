package tlsg

import (
	"log"
	"sync"
	"time"
	"treeless/src/com"
)

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string      //Physcal address
	LastHeartbeat time.Time   //Last time a heartbeat was listened
	HeldChunks    []int       //List of all chunks that this server holds
	conn          *tlcom.Conn //TCP connection, it may not exists
	sync.RWMutex
}

func (s *VirtualServer) needConnection() (err error) {
	s.RLock()
	for s.conn == nil {
		s.RUnlock()
		s.Lock()
		if s.conn == nil {
			s.conn, err = tlcom.CreateConnection(s.Phy)
			if err != nil {
				s.Unlock()
				return err
			}
			//Connection established
		}
		s.Unlock()
		s.RLock()
	}
	return nil
}
func (s *VirtualServer) freeConnection(cerr error) {
	if cerr != nil {
		//Connection problem, close connetion now
		log.Println("Connection problem", cerr)
		s.RUnlock()
		s.Lock()
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		s.Unlock()
		return
	}
	s.RUnlock()
}

//Get the value of key
func (s *VirtualServer) Get(key []byte) []byte {
	if err := s.needConnection(); err != nil {
		return nil
	}
	v, cerr := s.conn.Get(key)
	s.freeConnection(cerr)
	return v
}

//Set a new key/value pair
func (s *VirtualServer) Set(key, value []byte) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Set(key, value)
	s.freeConnection(cerr)
	return cerr
}

//Del deletes a key/value pair
func (s *VirtualServer) Del(key []byte) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Del(key)
	s.freeConnection(cerr)
	return cerr
}

func (s *VirtualServer) Transfer(addr string, chunkID int) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Transfer(addr, chunkID)
	s.freeConnection(cerr)
	return cerr
}

//GetAccessInfo request DB access info
func (s *VirtualServer) GetAccessInfo() []byte {
	if err := s.needConnection(); err != nil {
		return nil
	}
	v, cerr := s.conn.GetAccessInfo()
	s.freeConnection(cerr)
	return v
}

//AddServerToGroup request to add this server to the server group
func (s *VirtualServer) AddServerToGroup(addr string) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.AddServerToGroup(addr)
	s.freeConnection(cerr)
	return cerr
}

//GetChunkInfo request chunk info
func (s *VirtualServer) GetChunkInfo(chunkID int) (size uint64) {
	if err := s.needConnection(); err != nil {
		return 0
	}
	v, cerr := s.conn.GetChunkInfo(chunkID)
	s.freeConnection(cerr)
	return v
}
