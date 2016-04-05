package heartbeat

import (
	"log"
	"sync/atomic"
	"time"
	"treeless/com/udpconn"
	"treeless/dist/servergroup"
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
	timeouts       map[string]int
	sg             *servergroup.ServerGroup
}

//Stop requesting and listening to heartbeats
func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

func (h *Heartbeater) request(addr string) (ok bool) {
	aa, err := udpconn.Request(addr, heartbeatTimeout)
	if err != nil {
		if h.sg.IsServerOnGroup(addr) {
			h.timeouts[addr] = h.timeouts[addr] + 1
			if h.timeouts[addr] > 3 {
				delete(h.timeouts, addr)
				//Server is dead
				log.Println("Server is dead:", addr)
				h.sg.RemoveServer(addr)
			}
		} else {
			delete(h.timeouts, addr)
		}
		return false
	}
	delete(h.timeouts, addr)
	if !h.sg.IsServerOnGroup(addr) {
		h.sg.AddServerToGroup(addr)
	}
	h.sg.SetServerChunks(addr, aa.KnownChunks)
	//Detect added servers
	for _, s := range aa.KnownServers {
		if !h.sg.IsServerOnGroup(s) {
			h.request(s)
		}
	}
	return true
}

//Start a new heartbeater in the background and introduce the changes into sg
//It blocks until the first heartbeat of each server is served
func Start(sg *servergroup.ServerGroup) *Heartbeater {
	h := new(Heartbeater)
	h.timeouts = make(map[string]int)
	h.sg = sg
	for _, s := range sg.Servers() {
		h.request(s.Phy)
	}
	go func() {
		ticker := time.NewTicker(heartbeatSleep)
		defer ticker.Stop()
		for atomic.LoadInt32(&h.stop) == 0 {
			queryList := sg.Servers()
			if len(queryList) == 0 {
				log.Println("Heartbeat querylist empty")
				return
			}
			for _, qs := range queryList {
				for i := 0; i < 5; i++ {
					if h.request(qs.Phy) {
						break
					}
				}
				<-ticker.C
			}
		}
	}()
	return h
}
