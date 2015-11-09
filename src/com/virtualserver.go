package tlcom

import "time"

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string      //Physcal address
	lastHeartbeat time.Time   //Last time a heartbeat was listened
	heldChunks    []int       //List of all chunks that this server holds
	conn          *ClientConn //TCP connection, it may not exists
}

//NeedConnection tries to create a connection to the server
func (s *VirtualServer) NeedConnection() (err error) {
	if s.conn == nil || s.conn.isClosed() {
		s.conn, err = CreateConnection(s.Phy)
		return err
	}
	return nil
}
