package tlheartbeat

import (
	"log"
	"sync/atomic"
	"time"
	"treeless/src/tlcom/udp"
	"treeless/src/tlsg"
)

var heartbeatTimeout = time.Millisecond * 500
var heartbeatSleep = time.Millisecond * 1000
var heartbeatSleepOnFail = time.Millisecond * 500

func watchdog(sg *tlsg.ServerGroup, addr string, chunkUpdateChannel chan int, stop *int32) {
	timeouts := 0
	for atomic.LoadInt32(stop) == 0 {
		aa, err := tlUDP.Request(addr, heartbeatTimeout)
		if err == nil {
			timeouts = 0
			//Detect added servers
			for _, s := range aa.KnownServers {
				err := sg.AddServerToGroup(s)
				if err == nil {
					go watchdog(sg, s, chunkUpdateChannel, stop)
				}
			}
			changes := sg.SetServerChunks(addr, aa.KnownChunks)
			for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
				chunkUpdateChannel <- changes[i]
			}
			time.Sleep(heartbeatSleep)
		} else {
			//log.Println("Server timeouted:", addr)
			timeouts++
			if timeouts == 5 {
				//Server is dead
				log.Println("Server is dead:", addr)
				sg.RemoveServer(addr)
				return
			}
			time.Sleep(heartbeatSleepOnFail)
		}
	}
}

func Start(sg *tlsg.ServerGroup, chunkUpdateChannel chan int) (stop func()) {
	var shouldStop int32
	shouldStop = 0
	stop = func() {
		atomic.StoreInt32(&shouldStop, 1)
	}
	//Init
	for _, s := range sg.Servers() {
		addr := s.Phy
		aa, err := tlUDP.Request(addr, heartbeatTimeout)
		if err == nil {
			//Detect added servers
			for _, s := range aa.KnownServers {
				err := sg.AddServerToGroup(s)
				if err == nil {
					go watchdog(sg, s, chunkUpdateChannel, &shouldStop)
				}
			}
			changes := sg.SetServerChunks(addr, aa.KnownChunks)
			for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
				chunkUpdateChannel <- changes[i]
			}
		}
		go watchdog(sg, s.Phy, chunkUpdateChannel, &shouldStop)
	}
	return stop
}
