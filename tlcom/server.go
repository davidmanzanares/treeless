package tlcom

import (
	"fmt"
	"net"
	"sync/atomic"
	"treeless/tlcom/tlproto"
	"treeless/tlcom/udp"
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

type TCPCallback func(write chan<- tlproto.Message, read <-chan tlproto.Message)
type UDPCallback func() tlUDP.AmAlive

//Start a Treeless server
func Start(addr string, localIP string, localport int, worker func(tlproto.Message) (response tlproto.Message), udpc UDPCallback) *Server {
	var s Server
	//Init server
	s.localIP = localIP
	listenConnections(&s, localport, worker)
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

func listenRequests(conn *net.TCPConn, id int, worker func(tlproto.Message) (response tlproto.Message)) {
	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	ch := make(chan tlproto.Message, 1024)
	toWorld := tlproto.NewBufferedConn(conn, ch)
	go func() {
		for {
			m, ok := <-ch
			if ok {
				response := worker(m)
				if response.Type != tlproto.OpNil {
					toWorld <- response
				}
			} else {
				//log.Println("Server: connection closed", conn.RemoteAddr())
				close(toWorld)
				return
			}
		}
	}()

	//fmt.Println("Server", conn.LocalAddr(), "listening")

}

func listenConnections(s *Server, port int, worker func(tlproto.Message) (response tlproto.Message)) {
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
