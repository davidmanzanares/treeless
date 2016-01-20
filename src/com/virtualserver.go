package tlcom

import "time"

//VirtualServer stores generical server info
type VirtualServer struct {
	Phy           string    //Physcal address
	LastHeartbeat time.Time //Last time a heartbeat was listened
	HeldChunks    []int     //List of all chunks that this server holds
	Conn          *Conn     //TCP connection, it may not exists
}

//NeedConnection tries to create a connection to the server if needed
func (s *VirtualServer) NeedConnection() (err error) {
	if s.Conn == nil {
		s.Conn, err = CreateConnection(s.Phy)
		return err
	}
	return nil
}
