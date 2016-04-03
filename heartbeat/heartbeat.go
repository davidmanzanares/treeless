package heartbeat

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"treeless/servergroup"
	"treeless/tlcom/udp"
)

var heartbeatTimeout = time.Millisecond * 500
var heartbeatSleep = time.Millisecond * 500
var timeoutRetries = 3

//Heartbeater is used to discover changes in the DB topology by using a ping-pong protocol
//Exported fields are used to configure various parameters
type Heartbeater struct {
	Sleep          time.Duration
	SleepOnFail    time.Duration
	Timeout        time.Duration
	TimeoutRetries time.Duration
	stop           int32
}

//Stop requesting and listening to heartbeats
func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

//Start a new heartbeater in the background and introduce the changes into sg
//It blocks until the first heartbeat of each server is served
func Start(sg *servergroup.ServerGroup) *Heartbeater {
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
			for _, qs := range queryList {
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
								sg.AddServerToGroup(s) //TODO fast addition
							}
						}
					}
					sg.SetServerChunks(addr, aa.KnownChunks)
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
