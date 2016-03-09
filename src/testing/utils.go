package tltest

import (
	"time"
	"treeless/src/tlclient"
)

type capability int

const (
	capKill = iota
	capRestart
	capDisconnect
	capReconnect
)

type testServer interface {
	//Node address
	addr() string

	//For node failure simulation
	create(numChunks, redundancy int) string
	assoc(addr string) string
	kill()
	//For network failure simulation
	disconnect()
	reconnect()

	//Each test will need only some of these capabilities, testServers may no implement all
	testCapability(c capability) bool
}

func waitForServer(addr string) bool {
	for i := 0; i < 50; i++ {
		time.Sleep(time.Millisecond * 50)
		client, err := tlclient.Connect(addr)
		if err == nil {
			client.Close()
			return true
		}
	}
	return false
}
