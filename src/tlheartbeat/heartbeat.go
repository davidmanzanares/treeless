package tlheartbeat

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
	"treeless/src/tlcom/udp"
	"treeless/src/tlsg"
)

var heartbeatTickDuration = time.Millisecond * 200
var heartbeatTimeout = time.Millisecond * 200

func NewStopper() (stop func(), shouldStop func() bool, stopped func()) {
	s := new(int32)
	w := new(sync.WaitGroup)
	w.Add(1)
	stop = func() {
		atomic.StoreInt32(s, 1)
		w.Wait()
	}
	shouldStop = func() bool {
		return atomic.LoadInt32(s) != 0
	}
	stopped = func() {
		w.Done()
	}
	return stop, shouldStop, stopped
}

func Start(sg *tlsg.ServerGroup, chunkUpdateChannel chan int) {
	//Initial hearbeat
	l := list.New()
	for _, s := range sg.Servers {
		l.PushFront(s.Phy)
		aa, err := tlUDP.Request(s.Phy, heartbeatTimeout)
		if err == nil {
			//Detect added servers
			for _, s := range aa.KnownServers {
				err := sg.AddServerToGroup(s)
				if err == nil {
					l.PushFront(s)
				}
			}
			changes := sg.SetServerChunks(s.Phy, aa.KnownChunks)
			for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
				chunkUpdateChannel <- changes[i]
			}
		}
	}
	go func() {
		for { //TODO
			time.Sleep(heartbeatTickDuration)
			if l.Len() == 0 {
				continue
			}
			addr := l.Front().Value.(string)
			//Request known chunks list
			aa, err := tlUDP.Request(addr, heartbeatTimeout)
			if err == nil {
				//Detect added servers
				for _, s := range aa.KnownServers {
					err := sg.AddServerToGroup(s)
					if err == nil {
						l.PushFront(s)
					}
				}
				changes := sg.SetServerChunks(addr, aa.KnownChunks)
				for i := 0; chunkUpdateChannel != nil && i < len(changes); i++ {
					chunkUpdateChannel <- changes[i]
				}
			} else {
				//log.Println("UDP request timeout. Server", s.Phy, "UDP error:", err)
			}
			//if sg.localhost == nil {
			//	fmt.Println(sg)
			//}
			l.MoveToBack(l.Front())
		}
	}()
}
