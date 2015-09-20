package tlcom

import (
	"net"
	"time"
)

const udpPort = 9877

func ReplyToPings() net.Conn {
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
			conn.WriteTo([]byte("pong"), addr)
		}
	}()
	return conn
}

func Ping(ip string, timeout time.Duration) error {
	conn, err := net.ListenUDP("udp", nil)
	defer conn.Close()
	if err != nil {
		return err
	}
	destAddr := &net.UDPAddr{Port: udpPort, IP: net.ParseIP(ip)}
	conn.SetDeadline(time.Now().Add(timeout))
	conn.WriteTo([]byte("ping"), destAddr)
	for {
		_, readAddr, err := conn.ReadFromUDP(nil)
		if err != nil {
			return err
		} else if readAddr.IP.Equal(destAddr.IP) {
			return nil
		}
	}
}
