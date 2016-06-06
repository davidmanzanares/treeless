package udpconn

import (
	"log"
	"net"
	"time"
	"treeless/com/protocol"
)

//ReplyCallback is a function type that should return an AmAlive message
type ReplyCallback func() protocol.AmAlive

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
			saa := callback()
			_, err = conn.WriteTo(saa.Marshal(), addr)
			if err != nil {
				log.Println(err)
			}
		}
	}()
	return conn
}

//Request sends a UDP request to ip with a timeout, it waits until the timeout for a response
func Request(addr string, timeout time.Duration) (response *protocol.AmAlive, err error) {
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	destAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(timeout))
	conn.WriteTo([]byte("ping"), destAddr)
	for {
		message := make([]byte, protocol.MaxShortHeartbeatSize)
		n, readAddr, err := conn.ReadFromUDP(message)
		if err != nil {
			return nil, err
		} else if readAddr.IP.Equal(destAddr.IP) {
			return protocol.AmAliveUnMarshal(message[:n])
		}
	}
}
