package servergroup

import (
	"sync"
	"time"
	"treeless/com"
	"treeless/com/protocol"
)

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string    //Physical address. READ-ONLY by external packages!!!
	lastHeartbeat time.Time //Last time a heartbeat was listened
	dead          bool
	heldChunks    []protocol.AmAliveChunk //List of all chunks that this server holds
	conn          *com.Conn             //TCP connection, it may not exists
	m             sync.RWMutex
}

func (s *VirtualServer) getChunk(cid int) *protocol.AmAliveChunk {
	for _, c := range s.heldChunks {
		if c.ID == cid {
			return &c
		}
	}
	return nil
}
func (s *VirtualServer) needConnection() (err error) {
	s.m.RLock()
	for i := 0; s.conn == nil; i++ {
		s.m.RUnlock()
		s.m.Lock()
		if s.conn == nil {
			//log.Println("Creatting conn to", s.Phy)
			s.conn, err = com.CreateConnection(s.Phy, func() {
				//log.Println("Free connection", s.Phy)
				s.freeConn()
			})
			//log.Println("Creatted conn to", s.Phy, "err:", err)
			if err != nil {
				s.m.Unlock()
				return err
			}
			//Connection established
		}
		s.m.Unlock()
		s.m.RLock()
	}
	return nil
}
func (s *VirtualServer) freeConn() {
	//Close connetion now
	s.m.Lock()
	//log.Println("Free connection", s.Phy)
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.m.Unlock()
}

//Get the value of key
func (s *VirtualServer) Get(key []byte, timeout time.Duration) (com.GetOperation, error) {
	if err := s.needConnection(); err != nil {
		return com.GetOperation{}, err
	}
	r := s.conn.Get(key, timeout)
	s.m.RUnlock()
	return r, nil
}

//Set a new key/value pair
func (s *VirtualServer) Set(key, value []byte, timeout time.Duration) (com.SetOperation, error) {
	if err := s.needConnection(); err != nil {
		return com.SetOperation{}, err
	}
	r := s.conn.Set(key, value, timeout)
	s.m.RUnlock()
	return r, nil
}

//Del deletes a key/value pair
func (s *VirtualServer) Del(key []byte, value []byte, timeout time.Duration) (com.DelOperation, error) {
	if err := s.needConnection(); err != nil {
		return com.DelOperation{}, err
	}
	r := s.conn.Del(key, value, timeout)
	s.m.RUnlock()
	return r, nil
}

//Set a new key/value pair
func (s *VirtualServer) CAS(key, value []byte, timeout time.Duration) (com.CASOperation, error) {
	if err := s.needConnection(); err != nil {
		return com.CASOperation{}, err
	}
	r := s.conn.CAS(key, value, timeout)
	s.m.RUnlock()
	return r, nil
}

func (s *VirtualServer) Transfer(addr string, chunkID int) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Transfer(addr, chunkID)
	s.m.RUnlock()
	return cerr
}

//GetAccessInfo request DB access info
func (s *VirtualServer) GetAccessInfo() []byte {
	if err := s.needConnection(); err != nil {
		return nil
	}
	v, _ := s.conn.GetAccessInfo()
	s.m.RUnlock()
	return v
}

//AddServerToGroup request to add this server to the server group
func (s *VirtualServer) AddServerToGroup(addr string) error {
	if err := s.needConnection(); err != nil {
		return err
	}
	cerr := s.conn.AddServerToGroup(addr)
	s.m.RUnlock()
	return cerr
}

func (s *VirtualServer) Protect(chunkID int) (ok bool) {
	if err := s.needConnection(); err != nil {
		return false
	}
	cerr := s.conn.Protect(chunkID)
	s.m.RUnlock()
	return cerr == nil
}

//GetChunkInfo request chunk info
func (s *VirtualServer) GetChunkInfo(chunkID int) (size uint64) {
	if err := s.needConnection(); err != nil {
		return 0
	}
	v, _ := s.conn.GetChunkInfo(chunkID)
	s.m.RUnlock()
	return v
}

func (s *VirtualServer) SetBuffered() (ok bool) {
	if err := s.needConnection(); err != nil {
		return false
	}
	s.conn.SetBuffered()
	s.m.RUnlock()
	return true
}

func (s *VirtualServer) SetNoDelay() (ok bool) {
	if err := s.needConnection(); err != nil {
		return false
	}
	s.conn.SetNoDelay()
	s.m.RUnlock()
	return true
}
