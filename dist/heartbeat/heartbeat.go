package heartbeat

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"treeless/com/buffconn"
	"treeless/com/protocol"
	"treeless/com/udpconn"
	"treeless/dist/servergroup"
	"treeless/local"
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
	core           *local.Core
	news           []protocol.Gossip
	newsTime       []time.Time
	newsMutex      sync.Mutex
}

//Stop requesting and listening to heartbeats
func (h *Heartbeater) Stop() {
	atomic.StoreInt32(&h.stop, 1)
}

func getaa(addr string) (*protocol.AmAlive, error) {
	d := &net.Dialer{Timeout: time.Second}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	readch := make(chan protocol.Message)
	writech := buffconn.NewBufferedConn(conn.(*net.TCPConn), readch)
	writech <- protocol.Message{Type: protocol.OpAmAliveRequest}
	select {
	case <-time.After(time.Second):
		conn.Close()
		close(writech)
		return nil, errors.New("Reseponse timeout")
	case m := <-readch:
		conn.Close()
		close(writech)
		return protocol.AmAliveUnMarshal(m.Value)
	}
}

func (h *Heartbeater) addNews(addr string, hash uint64) {
	h.cleanNews()
	if h.core != nil && addr == h.core.LocalhostIPPort {
		return
	}
	h.newsMutex.Lock()
	h.newsTime = append(h.newsTime, time.Now())
	h.news = append(h.news, protocol.Gossip{ServerAddr: addr, ServerHash: hash})
	h.newsMutex.Unlock()
}

func (h *Heartbeater) cleanNews() {
	t := time.Now()
	h.newsMutex.Lock()
	for i := 0; i < len(h.news); i++ {
		if h.newsTime[i].Add(time.Minute).Before(t) {
			h.news = h.news[1:]
			h.newsTime = h.newsTime[1:]
		} else {
			break
		}
	}
	h.newsMutex.Unlock()
}

func (h *Heartbeater) request(addr string) (ok bool) {
	if h.core != nil && addr == h.core.LocalhostIPPort {
		return false
	}
	saa, saaerr := udpconn.Request(addr, heartbeatTimeout)
	defer func() {
		//Gossip protocol
		if saaerr == nil {
			for _, g := range saa.News {
				gaddr := g.ServerAddr
				var ourAA protocol.AmAlive
				ourAA.KnownChunks = h.sg.GetServerChunks(gaddr)
				ourAA.KnownServers = h.sg.KnownServers()
				if g.ServerHash != ourAA.Short().Hash {
					//Mismatch detected, request more info
					log.Println("Gossip", addr, gaddr, h.request(gaddr))
				}
			}
		}
	}()
	if saaerr == nil {
		var ourAA protocol.AmAlive
		ourAA.KnownChunks = h.sg.GetServerChunks(addr)
		ourAA.KnownServers = h.sg.KnownServers()
		if saa.Hash == ourAA.Short().Hash {
			//Fast path
			return false
		}
	}
	//Slow path
	aa, err := getaa(addr)
	if err != nil {
		if h.sg.IsServerOnGroup(addr) {
			h.timeouts[addr] = h.timeouts[addr] + 1
			if h.timeouts[addr] > 3 {
				delete(h.timeouts, addr)
				//Server is dead
				log.Println("Server is dead:", addr)
				h.sg.DeadServer(addr)
				h.addNews(addr, 0)
			}
		} else {
			delete(h.timeouts, addr)
		}
		return true
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
	var ourAA protocol.AmAlive
	ourAA.KnownChunks = h.sg.GetServerChunks(addr)
	ourAA.KnownServers = h.sg.KnownServers()
	h.addNews(addr, ourAA.Short().Hash)
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
				//fmt.Println(sg)
				<-ticker.C
			}
		}
	}()
	return h
}

//ListenReply starts listening and repling to UDP heartbeat requests
func (h *Heartbeater) ListenReply(c *local.Core) func() protocol.ShortAmAlive {
	h.core = c
	return func() protocol.ShortAmAlive {
		h.cleanNews()
		var r protocol.AmAlive
		r.KnownChunks = c.KnownChunksList()
		r.KnownServers = h.sg.KnownServers()
		s := r.Short()
		h.newsMutex.Lock()
		s.News = make([]protocol.Gossip, len(h.news))
		copy(s.News, h.news)
		h.newsMutex.Unlock()
		//log.Println("UDP AA", r)
		return s
	}
}
