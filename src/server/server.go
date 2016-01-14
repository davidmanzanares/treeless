package tlserver

import (
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"
	"treeless/src/com"
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
	sg *tlcom.ServerGroup
	//Status
	stopped int32
}

//Start a Treeless server
func Start(addr string, localport string, redundancy int, dbpath string) *Server {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(tlLowCom.GetLocalIP() + ":" + localport + " ")
	//Recover: log and quit
	defer func() {
		if r := recover(); r != nil {
			log.Println("DB panic", r)
			panic(r)
		}
	}()

	//Launch core
	var err error
	var s Server
	s.coreDB = tlcore.Create(dbpath)
	s.m, err = s.coreDB.AllocMap("map1")
	if err != nil {
		panic(err)
	}
	//Servergroup initialization
	if addr == "" {
		//New DB group
		s.sg = tlcom.CreateServerGroup(len(s.m.Chunks), localport, redundancy)
	} else {
		//Associate to an existing DB group
		s.sg, err = tlcom.Associate(addr, localport)
		if err != nil {
			panic(err)
		}
	}
	//Init server
	listenConnections(&s, localport)
	iport, err := strconv.Atoi(localport)
	if err != nil {
		panic(err)
	}
	s.udpCon = tlLowCom.ReplyToPings(udpCreateReplier(s.sg), iport)
	//Rebalancer
	tlcom.Rebalance(s.sg)
	log.Println("Server boot-up completed")
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
	s.sg.Stop()
}

func (s *Server) LogInfo() {
	log.Println("Info log")
	log.Println(s.sg)
}

func udpCreateReplier(sg *tlcom.ServerGroup) tlLowCom.UDPReplyCallback {
	return func() []byte {
		var r tlcom.Keepalive
		for i := 0; i < sg.NumChunks; i++ {
			if sg.IsChunkPresent(i) {
				r.KnownChunks = append(r.KnownChunks, i)
			}
		}
		for _, s := range sg.Servers {
			r.KnownServers = append(r.KnownServers, s.Phy)
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
	//fmt.Println("Server", conn.LocalAddr(), "listening")

	processMessage := func(message tlLowCom.Message) {
		//fmt.Println("Server", conn.LocalAddr(), "message recieved", string(message.Key), string(message.Value))
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
		case tlLowCom.OpTransfer:
			var chunkID int
			err := json.Unmarshal(message.Key, &chunkID)
			if err != nil {
				panic(string(message.Key) + err.Error())
			}
			transferFail := func() {
				var response tlLowCom.Message
				response.ID = message.ID
				response.Type = tlLowCom.OpErr
				writeCh <- response
			}
			if s.sg.IsChunkPresent(chunkID) {
				//New goroutine will put every key value pair into destination, it will manage the OpOK response
				addr := string(message.Value)
				c, err := tlcom.CreateConnection(addr)
				if err != nil {
					log.Println(1111111111, err)
					transferFail()
				} else {
					go func(c *tlcom.ClientConn) {
						s.m.Iterate(chunkID, func(key, value []byte) {
							c.Put(key, value)
						})
						c.GetAccessInfo()
						c.Close()
						var response tlLowCom.Message
						response.ID = message.ID
						response.Type = tlLowCom.OpOK
						writeCh <- response
					}(c)
				}
			} else {
				transferFail()
			}

		case tlLowCom.OpGetConf:
			var response tlLowCom.Message
			response.ID = message.ID
			response.Type = tlLowCom.OpGetConfResponse
			b, err := s.sg.Marshal()
			if err != nil {
				panic(err)
			}
			response.Value = b
			writeCh <- response
		case tlLowCom.OpAddServerToGroup:
			var response tlLowCom.Message
			response.ID = message.ID
			err := s.sg.AddServerToGroup(string(message.Key))
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
