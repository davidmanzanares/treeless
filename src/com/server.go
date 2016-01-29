package tlcom

import (
	"fmt"
	"net"
	"sync/atomic"
	"treeless/src/com/tcp"
	"treeless/src/com/udp"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	//Net
	udpCon      net.Conn
	tcpListener *net.TCPListener
	//Status
	stopped int32
}

type TCPCallback func(write chan<- tlTCP.Message, read <-chan tlTCP.Message)
type UDPCallback func() tlUDP.AmAlive

//Start a Treeless server
func Start(addr string, localport int, tcpc TCPCallback, udpc UDPCallback) *Server {
	var s Server
	//Init server
	listenConnections(&s, localport, tcpc)
	s.udpCon = tlUDP.Reply(tlUDP.ReplyCallback(udpc), localport)
	return &s
}

//IsStopped returns true if the server is not running
func (s *Server) IsStopped() bool {
	return atomic.LoadInt32(&s.stopped) != 0
}

//Stop the server, close all TCP/UDP connections
func (s *Server) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.udpCon.Close()
	s.tcpListener.Close()
}

func listenRequests(conn *net.TCPConn, id int, tcpc TCPCallback) {
	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	readChannel, writeChannel := tlTCP.NewBufferedConn(conn)

	//fmt.Println("Server", conn.LocalAddr(), "listening")

	go tcpc(writeChannel, readChannel)
}

func listenConnections(s *Server, port int, tcpc TCPCallback) {
	taddr, err := net.ResolveTCPAddr("tcp", GetLocalIP()+":"+fmt.Sprint(port))
	if err != nil {
		panic(err)
	}
	ln, err := net.ListenTCP("tcp", taddr)
	if err != nil {
		panic(err)
	}
	s.tcpListener = ln
	go func(s *Server) {
		var tcpConnections []*net.TCPConn
		for i := 0; ; i++ {
			conn, err := s.tcpListener.AcceptTCP()
			if err != nil {
				for i := 0; i < len(tcpConnections); i++ {
					tcpConnections[i].Close()
				}
				if s.IsStopped() {
					return
				}
				panic(err)
			}
			tcpConnections = append(tcpConnections, conn)
			go listenRequests(conn, i, tcpc)
		}
	}(s)
}
