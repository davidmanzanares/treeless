package tlserver

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"time"
	"treeless/com"
	"treeless/core"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	coreDB      *tlcore.DB
	m           *tlcore.Map
	udpCon      net.Conn
	tcpListener *net.TCPListener
	stopped     int32
}

const bufferSize = 2048

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

//Start a Treeless server
func Start() *Server {
	//go http.ListenAndServe("localhost:8080", nil)
	//Recover
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()

	//Default values
	dbPath := "tmpDB"
	//Parse args
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			time.Sleep(time.Second * 1)
			fmt.Println("start")
			pprof.StartCPUProfile(f)
			time.Sleep(time.Second * 10)
			pprof.StopCPUProfile()
			f.Close()
			fmt.Println("fflushed")
		}()
	}
	//Launch core
	var s Server
	s.coreDB = tlcore.Create(dbPath)
	var err error
	s.m, err = s.coreDB.AllocMap("map1")
	if err != nil {
		panic(err)
	}
	//Init server
	listenConnections(&s)
	s.udpCon = tlcom.ReplyToPings(udpCreateReplier(&s))
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
	s.coreDB.Close()
}

func udpCreateReplier(s *Server) tlcom.UDPReplyCallback {
	return func() []byte {
		return []byte(fmt.Sprint(len(s.m.Chunks)))
	}
}

func listenRequests(conn *net.TCPConn, id int, s *Server) {
	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	writeCh := make(chan tlcom.Message, 1024)
	go tlcom.TCPWriter(conn, writeCh)

	processMessage := func(message tlcom.Message) {
		switch message.Type {
		case tlcore.OpGet:
			var response tlcom.Message
			rval, err := s.m.Get(message.Key)
			//fmt.Println("Get operation", key, rval, err)
			response.ID = message.ID
			if err != nil {
				response.Type = tlcom.OpGetResponseError
				response.Value = []byte(err.Error())
			} else {
				response.Type = tlcom.OpGetResponse
				response.Value = rval
			}
			writeCh <- response
		case tlcore.OpPut:
			s.m.Put(message.Key, message.Value)
			//TODO err response
			//fmt.Println("Put operation", message.Key, message.Value, err)
			//if err != nil {
			//	panic(err)
			//}
		}
	}

	tlcom.TCPReader(conn, processMessage)

	close(writeCh)

	conn.Close()
	//log.Println("Connection closed. Connection ID:", id)
}

func listenConnections(s *Server) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{Port: 9876})
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
			go listenRequests(conn, i, s)
		}
	}(s)
}
