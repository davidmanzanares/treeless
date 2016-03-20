package tlheartbeat

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"treeless/src/tlcom/udp"
	"treeless/src/tlsg"
)

var heartbeatTimeout = time.Millisecond * 1000
var heartbeatSleep = time.Millisecond * 500
var heartbeatSleepOnFail = time.Millisecond * 500
var timeoutRetries = 5

type Heartbeater struct {
	Sleep          time.Duration
	SleepOnFail    time.Duration
	Timeout        time.Duration
	TimeoutRetries time.Duration
	stop           int32
}

func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

func Start(sg *tlsg.ServerGroup, chunkUpdateChannel chan int) *Heartbeater {
	//Init
	h := new(Heartbeater)
	var w sync.WaitGroup //Wait for the first round
	w.Add(1)
	go func() {
		timeouts := make(map[string]int)
		ticker := time.NewTicker(heartbeatSleep)
		defer ticker.Stop()
		firstRun := true
		for atomic.LoadInt32(&h.stop) == 0 {
			queryList := sg.Servers()
			for i := 0; i < len(queryList); i++ {
				qs := queryList[i]
				addr := qs.Phy
				aa, err := tlUDP.Request(addr, heartbeatTimeout)
				if err == nil {
					delete(timeouts, qs.Phy)
					//Detect added servers
					for _, s := range aa.KnownServers {
						if !sg.IsServerOnGroup(s) {
							_, err := tlUDP.Request(s, heartbeatTimeout)
							if err == nil {
								//Add new server to queryList
								sg.AddServerToGroup(s)
							}
						}
					}
					changes := sg.SetServerChunks(addr, aa.KnownChunks)
					for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
						chunkUpdateChannel <- changes[i]
					}
				} else {
					timeouts[qs.Phy] = timeouts[qs.Phy] + 1
					if timeouts[qs.Phy] > 3 {
						//Server is dead
						log.Println("Server is dead:", qs.Phy)
						delete(timeouts, qs.Phy)
						sg.RemoveServer(qs.Phy)
					}
				}
				if !firstRun {
					<-ticker.C
				}
			}
			if firstRun {
				w.Done()
				firstRun = false
			}
			if len(queryList) == 0 {
				log.Println("Heartbeat querylist empty")
				return
			}
		}
	}()
	w.Wait()
	return h
}
