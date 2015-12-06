package tlcom

import "time"

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string      //Physcal address
	LastHeartbeat time.Time   //Last time a heartbeat was listened
	HeldChunks    []int       //List of all chunks that this server holds
	Conn          *ClientConn //TCP connection, it may not exists
}

//NeedConnection tries to create a connection to the server
func (s *VirtualServer) NeedConnection() (err error) {
	if s.Conn == nil || s.Conn.isClosed() {
		s.Conn, err = CreateConnection(s.Phy)
		return err
	}
	return nil
}
