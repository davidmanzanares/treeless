package tlsg

import (
	"sync"
	"time"
	"treeless/src/com"
)

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string      //Physcal address
	LastHeartbeat time.Time   //Last time a heartbeat was listened
	HeldChunks    []int       //List of all chunks that this server holds
	Conn          *tlcom.Conn //TCP connection, it may not exists
	sync.Mutex
}

//NeedConnection tries to create a connection to the server if needed
func (s *VirtualServer) NeedConnection() (err error) {
	s.Lock()
	defer s.Unlock()
	if s.Conn == nil || s.Conn.IsClosed() {
		s.Conn, err = tlcom.CreateConnection(s.Phy)
		return err
	}
	return nil
}
