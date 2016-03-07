package tlsg

import (
	"log"
	"sync"
	"time"
	"treeless/src/tlcom"
)

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy string //Physcal address

	//TODO simplify
	lastHeartbeat time.Time   //Last time a heartbeat was listened
	heldChunks    []int       //List of all chunks that this server holds
	conn          *tlcom.Conn //TCP connection, it may not exists
	M             sync.RWMutex
}

func (s *VirtualServer) needConnection() (err error) {
	s.M.RLock()
	for i := 0; s.conn == nil; i++ {
		s.M.RUnlock()
		s.M.Lock()
		if s.conn == nil {
			//log.Println("Creatting conn to", s.Phy)
			s.conn, err = tlcom.CreateConnection(s.Phy)
			//log.Println("Creatted conn to", s.Phy, "err:", err)
			if err != nil {
				s.M.Unlock()
				return err
			}
			//Connection established
		}
		s.M.Unlock()
		s.M.RLock()
	}
	return nil
}
func (s *VirtualServer) freeConnection(cerr error) {
	if cerr != nil {
		//Connection problem, close connetion now
		log.Println("Connection problem", cerr)
		s.M.RUnlock()
		log.Println("Connection problem try lock")
		s.M.Lock()
		log.Println("Connection problem locked")
		if s.conn != nil {
			log.Println("Connection problem goto close")
			s.conn.Close()
			log.Println("Connection problem closed")
			s.conn = nil
		}
		s.M.Unlock()
		log.Println("Connection problem: connection closed", s.Phy)
		return
	}
	s.M.RUnlock()
}

func (s *VirtualServer) Timeout() {
	log.Println("timeout try lock", s.M)
	s.M.Lock()
	log.Println("timeout locked")
	if s.conn != nil {
		log.Println("timeout closing")
		s.conn.Close()
		s.conn = nil
	}
	s.M.Unlock()
	log.Println("timeout unlocked")
}

//Get the value of key
//Caller must issue a s.RUnlock() after using the channel
func (s *VirtualServer) Get(key []byte, timeout time.Duration) chan tlcom.Result {
	if err := s.needConnection(); err != nil {
		return nil
	}
	r := s.conn.Get(key, timeout)
	s.M.RUnlock()
	return r
}

//Set a new key/value pair
func (s *VirtualServer) Set(key, value []byte, timeout time.Duration) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Set(key, value, timeout)
	s.freeConnection(cerr)
	return cerr
}

//Del deletes a key/value pair
func (s *VirtualServer) Del(key []byte, timeout time.Duration) error {
	if err := s.needConnection(); err != nil {
		return nil
	}
	cerr := s.conn.Del(key, timeout)
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

func (s *VirtualServer) Protect(chunkID int) (ok bool) {
	if err := s.needConnection(); err != nil {
		return false
	}
	cerr := s.conn.Protect(chunkID)
	s.freeConnection(cerr)
	return cerr == nil
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
