package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"
	"treeless/src/com/lowcom"
	"treeless/src/core"
)

//Server listen to TCP & UDP, accepting connections and responding to clients
type Server struct {
	//Core
	coreDB *tlcore.DB
	m      *tlcore.Map
	//Net
	udpCon      net.Conn
	tcpListener *net.TCPListener
	//Distribution
	sg *ServerGroup
	//Status
	stopped int32
}

//Start a Treeless server
func Start(newGroup bool, addr string, port string, redundancy int) *Server {
	//Recover
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()

	//Default values
	dbPath := "tmpDB" + tlLowCom.GetLocalIP() + ":" + port
	//Launch core
	var s Server
	s.coreDB = tlcore.Create(dbPath)
	var err error
	if newGroup {
		s.m, err = s.coreDB.AllocMap("map1")
		if err != nil {
			panic(err)
		}
		s.sg = CreateServerGroup(len(s.m.Chunks), port, redundancy)
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
	s.coreDB.Close()
}

func udpCreateReplier(s *Server) tlLowCom.UDPReplyCallback {
	return func() []byte {
		var r []int
		for i := 0; i < len(s.m.Chunks); i++ {
			if s.m.Chunks[i] != nil {
				r = append(r, i)
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
		case tlLowCom.OpGetChunkInfo:
			var response tlLowCom.Message
			response.ID = message.ID
			c := s.m.Chunks[binary.LittleEndian.Uint32(message.Key)]
			response.Type = tlLowCom.OpGetChunkInfoResponse
			response.Value = make([]byte, 8)
			binary.LittleEndian.PutUint64(response.Value, c.St.Length+1)
			writeCh <- response
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
	taddr, err := net.ResolveTCPAddr("tcp", tlLowCom.GetLocalIP()+":"+port)
	if err != nil {
		panic(err)
	}
	ln, err := net.ListenTCP("tcp", taddr)
	if err != nil {
		panic(err)
	}
	log.Println("Listening on:", ln.Addr())
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
