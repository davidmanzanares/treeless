package tlcom

import (
	"hash/fnv"
	"net"
	"strconv"
	"sync/atomic"
	"time"
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

type TCPCallback func(message tlTCP.Message, responseChannel chan tlTCP.Message)
type UDPCallback func() tlUDP.AmAlive

//Start a Treeless server
func Start(addr string, localport string, tcpc TCPCallback, udpc UDPCallback) *Server {
	var s Server
	//Init server
	listenConnections(&s, localport, tcpc)
	iport, err := strconv.Atoi(localport)
	if err != nil {
		panic(err)
	}
	s.udpCon = tlUDP.Reply(tlUDP.ReplyCallback(udpc), iport)
	return &s
}

func chunkHash(x int, y time.Time) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(time.Now().String()))
	return hasher.Sum64()
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

func listenRequests(conn *net.TCPConn, id int, s *Server, tcpc TCPCallback) {
	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	writeCh := make(chan tlTCP.Message, 1024)
	go tlTCP.Writer(conn, writeCh)
	//fmt.Println("Server", conn.LocalAddr(), "listening")

	processMessage := func(message tlTCP.Message) {
		tcpc(message, writeCh)
	}

	tlTCP.Reader(conn, processMessage)

	close(writeCh)

	conn.Close()
	//log.Println("Connection closed. Connection ID:", id)
}

func listenConnections(s *Server, port string, tcpc TCPCallback) {
	taddr, err := net.ResolveTCPAddr("tcp", GetLocalIP()+":"+port)
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
			go listenRequests(conn, i, s, tcpc)
		}
	}(s)
}
