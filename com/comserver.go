package tlcom

import (
	"fmt"
	"net"
	"sync/atomic"
	"treeless/com/buffconn"
	"treeless/com/protocol"
	"treeless/com/udpconn"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	localIP string
	//Net
	udpCon      net.Conn
	tcpListener *net.TCPListener
	//Status
	stopped int32
}

type TCPCallback func(write chan<- protocol.Message, read <-chan protocol.Message)
type UDPCallback func() protocol.ShortAmAlive

//Start a Treeless server
func Start(addr string, localIP string, localport int, worker func(protocol.Message) (response protocol.Message), udpc UDPCallback) *Server {
	var s Server
	//Init server
	s.localIP = localIP
	listenConnections(&s, localport, worker)
	s.udpCon = udpconn.Reply(udpconn.ReplyCallback(udpc), localport)
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

func listenRequests(conn *net.TCPConn, id int, worker func(protocol.Message) (response protocol.Message)) {
	//log.Println("New connection accepted. Connection ID:", id)
	go func() {
		c := buffconn.New(conn)
		defer c.Close()
		for {
			msg, err := c.Read()
			if err == nil {
				response := worker(msg)
				if response.Type > 0 {
					//fmt.Println(msg.Type, response.Type, conn.LocalAddr().String(), conn.RemoteAddr().String())
					c.Write(response)
				}
			} else {
				//log.Println("Server: connection closed", conn.RemoteAddr())
				return
			}
		}
	}()
}

func listenConnections(s *Server, port int, worker func(protocol.Message) (response protocol.Message)) {
	taddr, err := net.ResolveTCPAddr("tcp", s.localIP+":"+fmt.Sprint(port))
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
			//log.Println("TCP Accept", conn, "ASD", conn.LocalAddr(), conn.RemoteAddr())
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
			go listenRequests(conn, i, worker)
		}
	}(s)
}
