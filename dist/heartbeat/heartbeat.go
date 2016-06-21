package heartbeat

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
	"github.com/dv343/treeless/com"
	"github.com/dv343/treeless/com/protocol"
	"github.com/dv343/treeless/core"
	"github.com/dv343/treeless/dist/servergroup"
)

var heartbeatTimeout = time.Millisecond * 250
var heartbeatSleep = time.Millisecond * 500
var GossipNewsExpirationTime = time.Second * 10
var timeoutRetries = 3

//Heartbeater is used to discover changes in the DB topology by using a ping-pong protocol
//Exported fields are used to configure various parameters
type Heartbeater struct {
	Sleep                time.Duration
	SleepOnFail          time.Duration
	Timeout              time.Duration
	TimeoutRetries       time.Duration
	stop                 int32
	timeouts             map[string]int
	sg                   *servergroup.ServerGroup
	core                 *core.Core
	recentlyAddedServers map[string]time.Time
	recentlyDeadServers  map[string]time.Time
	newsMutex            sync.Mutex
}

//Stop requesting and listening to heartbeats
func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

func (h *Heartbeater) GossipAdded(addr string) {
	h.cleanNews()
	h.newsMutex.Lock()
	h.recentlyAddedServers[addr] = time.Now()
	h.newsMutex.Unlock()
}

func (h *Heartbeater) GossipDead(addr string) {
	h.cleanNews()
	h.newsMutex.Lock()
	h.recentlyDeadServers[addr] = time.Now()
	h.newsMutex.Unlock()
}

func (h *Heartbeater) cleanNews() {
	t := time.Now()
	h.newsMutex.Lock()
	for k, v := range h.recentlyAddedServers {
		if v.Add(GossipNewsExpirationTime).Before(t) {
			delete(h.recentlyAddedServers, k)
		}
	}
	for k, v := range h.recentlyDeadServers {
		if v.Add(GossipNewsExpirationTime).Before(t) {
			delete(h.recentlyDeadServers, k)
		}
	}
	h.newsMutex.Unlock()
}

func (h *Heartbeater) request(addr string) (ok bool) {
	if addr == h.sg.LocalhostIPPort {
		return true
	}
	aa, err := com.UDPRequest(addr, heartbeatTimeout)
	if err != nil {
		if h.sg.IsServerOnGroup(addr) {
			h.timeouts[addr] = h.timeouts[addr] + 1
			if h.timeouts[addr] >= 3 {
				delete(h.timeouts, addr)
				//Server is dead
				h.sg.DeadServer(addr)
				h.GossipDead(addr)
			} else {
				return h.request(addr)
			}
		} else {
			delete(h.timeouts, addr)
		}
		return false
	}
	//Process
	delete(h.timeouts, addr)
	if !h.sg.IsServerOnGroup(addr) {
		h.sg.AddServerToGroup(addr)
		h.GossipAdded(addr)
	}
	h.sg.SetServerChunks(addr, aa.KnownChunks)
	//Gossip
	if err == nil {
		for _, new := range aa.RecentlyAddedServers {
			if !h.sg.IsServerOnGroup(new) {
				//Mismatch detected, request more info
				log.Println("Gossip (add)", addr, new, h.request(new))
			}
		}
		for _, new := range aa.RecentlyDeadServers {
			if h.sg.IsServerOnGroup(new) {
				//Mismatch detected, request more info
				log.Println("Gossip (dead)", addr, new, h.request(new))
			}
		}
	}
	return true
}

//Start a new heartbeater in the background and introduce the changes into sg
//It blocks until the first heartbeat of each server is served
func Start(sg *servergroup.ServerGroup) *Heartbeater {
	h := new(Heartbeater)
	h.timeouts = make(map[string]int)
	h.recentlyAddedServers = make(map[string]time.Time)
	h.recentlyDeadServers = make(map[string]time.Time)
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
			}
			for _, qs := range queryList {
				h.request(qs.Phy)
				<-ticker.C
			}
		}
	}()
	return h
}

//ListenReply starts listening and repling to UDP heartbeat requests
func (h *Heartbeater) ListenReply(c *core.Core) func() protocol.AmAlive {
	h.core = c
	return func() (r protocol.AmAlive) {
		h.cleanNews()
		r.KnownChunks = c.PresentChunksList()
		h.cleanNews()
		h.newsMutex.Lock()
		r.RecentlyAddedServers = make([]string, 0, len(h.recentlyAddedServers))
		for k := range h.recentlyAddedServers {
			r.RecentlyAddedServers = append(r.RecentlyAddedServers, k)
		}
		r.RecentlyDeadServers = make([]string, 0, len(h.recentlyDeadServers))
		for k := range h.recentlyDeadServers {
			r.RecentlyDeadServers = append(r.RecentlyDeadServers, k)
		}
		h.newsMutex.Unlock()
		return r
	}
}
