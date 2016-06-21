package com

import (
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"github.com/dv343/treeless/com/buffconn"
	"github.com/dv343/treeless/com/protocol"
)

//Server listen to TCP & UDP, accepting connections and responding to clients using callback functions
type Server struct {
	localIP string
	//Net
	tcpListener *net.TCPListener
	udpListener *net.UDPConn
	//Status
	stopped int32
}

//TCPCallback is the main callback, it returns a response message, if the response message type is 0 the response will be dropped
type TCPCallback func(protocol.Message) (response protocol.Message)

//UDPCallback function should respond to incoming UDP pings
type UDPCallback func() protocol.AmAlive

//Start a Treeless server
func Start(localIP string, localPort int, tcpCallback func(protocol.Message) (response protocol.Message), udpCallback UDPCallback) *Server {
	s := new(Server)
	s.localIP = localIP
	listenUDP(s, udpCallback, localPort)
	listenTCP(s, tcpCallback, localPort)
	return s
}

//IsStopped returns true if the server is not running
func (s *Server) IsStopped() bool {
	return atomic.LoadInt32(&s.stopped) != 0
}

//Stop the server, close all TCP/UDP connections
func (s *Server) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.tcpListener.Close()
	s.udpListener.Close()
}

/*
	TCP
*/
func listenRequests(conn *net.TCPConn, worker TCPCallback) {
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
func listenTCP(s *Server, callback TCPCallback, port int) {
	taddr, err := net.ResolveTCPAddr("tcp", s.localIP+":"+fmt.Sprint(port))
	if err != nil {
		panic(err)
	}
	s.tcpListener, err = net.ListenTCP("tcp", taddr)
	if err != nil {
		panic(err)
	}
	go func(s *Server) {
		var tcpConnections []*net.TCPConn
		for {
			conn, err := s.tcpListener.AcceptTCP()
			//log.Println("TCP Accept", conn, "ASD", conn.LocalAddr(), conn.RemoteAddr())
			if err != nil {
				for _, conn := range tcpConnections {
					conn.Close()
				}
				if s.IsStopped() {
					return
				}
				panic(err)
			}
			tcpConnections = append(tcpConnections, conn)
			go listenRequests(conn, callback)
		}
	}(s)
}

/*
	UDP
*/
func listenUDP(s *Server, callback UDPCallback, udpPort int) *net.UDPConn {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: udpPort})
	s.udpListener = conn
	if err != nil {
		panic(err)
	}
	go func(s *Server) {
		for {
			_, addr, err := conn.ReadFromUDP(nil)
			if err != nil {
				conn.Close()
				if s.IsStopped() {
					return
				}
				panic(err)
			}
			saa := callback()
			_, err = conn.WriteTo(saa.Marshal(), addr)
			if err != nil {
				log.Println(err)
			}
		}
	}(s)
	return conn
}
