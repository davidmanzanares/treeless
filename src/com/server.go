package tlcom

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"
	"treeless/src/com/lowcom"
	"treeless/src/core"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	coreDB      *tlcore.DB
	m           *tlcore.Map
	udpCon      net.Conn
	tcpListener *net.TCPListener
	stopped     int32
	sg          *ServerGroup
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

//Start a Treeless server
func Start(newGroup bool, addr string, port string) *Server {
	//go http.ListenAndServe("localhost:8080", nil)
	//Recover
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()

	//Default values
	dbPath := "tmpDB" + getLocalIP() + port
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
	if newGroup {
		s.m, err = s.coreDB.AllocMap("map1")
		if err != nil {
			panic(err)
		}
		s.sg = CreateServerGroup(len(s.m.Chunks), port)
	} else {
		s.m, err = s.coreDB.DefineMap("map1")
		if err != nil {
			panic(err)
		}
		s.sg, err = ConnectAsServer(addr, port)
		if err != nil {
			panic(err)
		}
	}
	//Init server
	listenConnections(&s, port)
	iport, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	s.udpCon = tlLowCom.ReplyToPings(udpCreateReplier(&s), iport)
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

func udpCreateReplier(s *Server) tlLowCom.UDPReplyCallback {
	return func() []byte {
		var r tlLowCom.UDPResponse
		r.HeldChunks = make([]int, 0, len(s.m.Chunks))
		for i := 0; i < len(s.m.Chunks); i++ {
			if s.m.Chunks[i] != nil {
				r.HeldChunks = append(r.HeldChunks, i)
			}
		}
		b, err := json.Marshal(r)
		if err != nil {
			panic(err)
		}
		return b
	}
}

func listenRequests(conn *net.TCPConn, id int, s *Server) {
	//log.Println("New connection accepted. Connection ID:", id)
	//tcpWriter will buffer TCP writes to send more message in less TCP packets
	//this technique allows bigger throughtputs, but latency in increased a little
	writeCh := make(chan tlLowCom.Message, 1024)
	go tlLowCom.TCPWriter(conn, writeCh)

	processMessage := func(message tlLowCom.Message) {
		switch message.Type {
		case tlcore.OpGet:
			var response tlLowCom.Message
			rval, err := s.m.Get(message.Key)
			//fmt.Println("Get operation", key, rval, err)
			response.ID = message.ID
			if err != nil {
				response.Type = tlLowCom.OpGetResponseError
				response.Value = []byte(err.Error())
			} else {
				response.Type = tlLowCom.OpGetResponse
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
		case tlLowCom.OpGetConf:
			var response tlLowCom.Message
			response.ID = message.ID
			response.Type = tlLowCom.OpGetConfResponse
			b, _ := s.sg.Marshal()
			response.Value = b
			writeCh <- response
		case tlLowCom.OpAddServerToGroup:
			var response tlLowCom.Message
			response.ID = message.ID
			err := s.sg.addServerToGroup(string(message.Key))
			if err != nil {
				response.Type = tlLowCom.OpErr
				response.Value = []byte(err.Error())
				writeCh <- response
			} else {
				response.Type = tlLowCom.OpAddServerToGroupACK
				writeCh <- response
			}
		default:
			var response tlLowCom.Message
			response.ID = message.ID
			response.Type = tlLowCom.OpErr
			response.Value = []byte("Operation not supported")
			writeCh <- response
		}
	}

	tlLowCom.TCPReader(conn, processMessage)

	close(writeCh)

	conn.Close()
	//log.Println("Connection closed. Connection ID:", id)
}

func listenConnections(s *Server, port string) {
	taddr, err := net.ResolveTCPAddr("tcp", getLocalIP()+":"+port)
	if err != nil {
		panic(err)
	}
	ln, err := net.ListenTCP("tcp", taddr)
	if err != nil {
		panic(err)
	}
	log.Println("TCP listening on:", ln.Addr())
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
