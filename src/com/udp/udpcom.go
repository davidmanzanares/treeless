package tlUDP

import (
	"encoding/json"
	"log"
	"net"
	"time"
)

type AmAlive struct {
	KnownChunks  []int    //Chunks known by the server
	KnownServers []string //Servers known by the server
}

const maxMessageSize = 1024 * 16

//ReplyCallback is a function type that should return an AmAlive message
type ReplyCallback func() AmAlive

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
			aa := callback()
			_, err = conn.WriteTo(aa.marshal(), addr)
			if err != nil {
				log.Println(err)
			}
		}
	}()
	return conn
}

//Request sends a UDP request to ip with a timeout, it waits until the timeout for a response
func Request(addr string, timeout time.Duration) (response *AmAlive, err error) {
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
			return amAliveUnMarshal(message[:n])
		}
	}
}

func (aa *AmAlive) marshal() []byte {
	s, err := json.Marshal(aa)
	if err != nil {
		panic(err)
	}
	return s
}

func amAliveUnMarshal(s []byte) (*AmAlive, error) {
	var aa AmAlive
	err := json.Unmarshal(s, &aa)
	return &aa, err
}
