package tlcom

import (
	"net"
	"time"
)

const udpPort = 9877
const maxUDPMessageSize = 1024 * 16

//UDPReplyCallback is a function type that should return the server status in []byte form
type UDPReplyCallback func() []byte

//ReplyToPings listens and response to UDP requests
func ReplyToPings(callback UDPReplyCallback) net.Conn {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: udpPort})
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			_, addr, err := conn.ReadFromUDP(nil)
			if err != nil {
				conn.Close()
				return
			}
			conn.WriteTo(callback(), addr)
		}
	}()
	return conn
}

//UDPRequest sends a UDP request to ip with a timeout
func UDPRequest(ip string, timeout time.Duration) (response []byte, err error) {
	conn, err := net.ListenUDP("udp", nil)
	defer conn.Close()
	if err != nil {
		return nil, err
	}
	destAddr := &net.UDPAddr{Port: udpPort, IP: net.ParseIP(ip)}
	conn.SetDeadline(time.Now().Add(timeout))
	conn.WriteTo([]byte("ping"), destAddr)
	for {
		message := make([]byte, maxUDPMessageSize)
		n, readAddr, err := conn.ReadFromUDP(message)
		if err != nil {
			return nil, err
		} else if readAddr.IP.Equal(destAddr.IP) {
			return message[:n], nil
		}
	}
}
