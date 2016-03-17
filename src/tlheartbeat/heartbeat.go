package tlheartbeat

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
	"treeless/src/tlcom/udp"
	"treeless/src/tlsg"
)

var heartbeatTimeout = time.Millisecond * 1000
var heartbeatSleep = time.Millisecond * 1000
var heartbeatSleepOnFail = time.Millisecond * 500
var timeoutRetries = 5

type Heartbeater struct {
	Sleep          time.Duration
	SleepOnFail    time.Duration
	Timeout        time.Duration
	TimeoutRetries time.Duration
	stop           int32
}

func watchdog(h *Heartbeater, sg *tlsg.ServerGroup, addr string, chunkUpdateChannel chan int) {
	timeouts := 0
	time.Sleep(time.Duration(rand.Int63n(int64(heartbeatSleep))))
	for atomic.LoadInt32(&h.stop) == 0 {
		aa, err := tlUDP.Request(addr, heartbeatTimeout)
		if err == nil {
			timeouts = 0
			//Detect added servers
			for _, s := range aa.KnownServers {
				if !sg.IsServerOnGroup(s) {
					go func(s string) {
						_, err := tlUDP.Request(s, heartbeatTimeout)
						if err == nil {
							err := sg.AddServerToGroup(s)
							if err == nil {
								go watchdog(h, sg, s, chunkUpdateChannel)
							}
						}
					}(s)
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
			if timeouts == timeoutRetries {
				//Server is dead
				log.Println("Server is dead:", addr)
				sg.RemoveServer(addr)
				return
			}
			time.Sleep(heartbeatSleepOnFail)
		}
	}
}

func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

func Start(sg *tlsg.ServerGroup, chunkUpdateChannel chan int) *Heartbeater {
	h := new(Heartbeater)
	//Init
	for _, s := range sg.Servers() {
		addr := s.Phy
		aa, err := tlUDP.Request(addr, heartbeatTimeout)
		if err == nil {
			//Detect added servers
			for _, s := range aa.KnownServers {
				err := sg.AddServerToGroup(s)
				if err == nil {
					go watchdog(h, sg, s, chunkUpdateChannel)
				}
			}
			changes := sg.SetServerChunks(addr, aa.KnownChunks)
			for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
				chunkUpdateChannel <- changes[i]
			}
		}
		go watchdog(h, sg, s.Phy, chunkUpdateChannel)
	}
	return h
}
