package tlUDP

import (
	"log"
	"net"
	"time"
)

const maxMessageSize = 1024 * 16

//ReplyCallback is a function type that should return the server status in []byte form
type ReplyCallback func() []byte

//Reply listens and response to UDP requests
func Reply(callback ReplyCallback, udpPort int) net.Conn {
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
			_, err = conn.WriteTo(callback(), addr)
			if err != nil {
				log.Println(err)
			}
		}
	}()
	return conn
}

//Request sends a UDP request to ip with a timeout, it waits until the timeout for a response
func Request(addr string, timeout time.Duration) (response []byte, err error) {
	conn, err := net.ListenUDP("udp", nil)
	defer conn.Close()
	if err != nil {
		return nil, err
	}
	destAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(timeout))
	conn.WriteTo([]byte("ping"), destAddr)
	for {
		message := make([]byte, maxMessageSize)
		n, readAddr, err := conn.ReadFromUDP(message)
		if err != nil {
			return nil, err
		} else if readAddr.IP.Equal(destAddr.IP) {
			return message[:n], nil
		}
	}
}
