package tlcom

import (
	"container/heap"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*VirtualChunk

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].timeToReview.Before(pq[j].timeToReview)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*VirtualChunk)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(c *VirtualChunk, t time.Time) {
	c.timeToReview = t
	heap.Fix(pq, c.index)
}

func Rebalance(sg *ServerGroup) chan *VirtualChunk {
	ch := sg.chunkUpdateChannel
	duplicate := duplicator(sg)

	//Constantly check for possible duplications to rebalance the servers,
	//servers should have the more or less the same work
	go func() {
		time.Sleep(time.Second * 100)
		sg.Lock()
		for {
			known := float64(len(sg.localhost.HeldChunks))
			total := float64(sg.NumChunks) * float64(sg.Redundancy)
			avg := total / float64(len(sg.Servers))
			if known < avg*0.95 {
				c := &sg.chunks[rand.Int31n(int32(sg.NumChunks))]
				if !c.Holders[sg.localhost] {
					log.Println("Duplicate to rebalance")
					duplicate(c)
				}
			}
			timetowait := 1.0 / (avg - known)
			if (avg-known) <= 0.0 || timetowait > 20 {
				timetowait = 20
			}
			log.Println("Time to wait:", timetowait, "Avg: ", avg, "Known:", known)
			sg.Unlock()
			time.Sleep(time.Duration(float64(time.Second) * timetowait))
			sg.Lock()
		}
	}()

	//Rebalance chunks with low redundancy
	//Rebalance algorithm:
	//	For each chunk with low redundancy:
	//		wait a random lapse of time
	//		check chunk status
	//		if still with low redundancy / noone claimed it:
	//			Claim it
	//			transfer it

	go func() {
		sg.Lock()
		pq := make(PriorityQueue, 0)
		heap.Init(&pq)
		inQueue := make(map[*VirtualChunk]bool)
		tick := time.Tick(time.Second * 10000000)
		for {
			if pq.Len() > 0 {
				//log.Println("time", pq[0].timeToReview)
				now := time.Now()
				timeToWait := pq[0].timeToReview.Sub(now)
				if now.After(pq[0].timeToReview) {
					timeToWait = time.Microsecond
				}
				tick = time.Tick(timeToWait)
			} else {
				tick = time.Tick(time.Second * 10)
			}
			sg.Unlock()
			select {
			case chunk := <-ch:
				sg.Lock()
				chunk.timeToReview = time.Now().Add(time.Microsecond * time.Duration(10*1000*1000*rand.Float32())) //TODO add variable server k
				//log.Println("Chunk eta:", chunk.timeToReview, "ID:", chunk.ID)
				if inQueue[chunk] {
					pq.update(chunk, time.Now())
				} else {
					heap.Push(&pq, chunk)
					inQueue[chunk] = true
				}
			case <-tick:
				sg.Lock()
				if len(pq) > 0 {
					c := heap.Pop(&pq).(*VirtualChunk)
					if len(c.Holders) < sg.Redundancy && c.Holders[sg.localhost] == false {
						duplicate(c)
					} else {
						//log.Println("Chunk duplication aborted: chunk not needed", c.ID, c, c.Holders[sg.localhost])
					}
					delete(inQueue, c)
				}
			}
		}
	}()
	return ch
}

func getFreeDiskSpace() uint64 {
	var stat syscall.Statfs_t
	wd, err := os.Getwd()
	if err != nil {
		log.Println("GetFreeDiskSpace error", err)
	}
	syscall.Statfs(wd, &stat)
	// Available blocks * size per block = available space in bytes
	return stat.Bavail * uint64(stat.Bsize)
}

func duplicator(sg *ServerGroup) (duplicate func(c *VirtualChunk)) {
	ch := make(chan *VirtualChunk, 1024)

	duplicate = func(c *VirtualChunk) {
		//Execute this code as soon as possible, adding the chunk to the known list is time critical
		log.Println(time.Now().String()+"Request chunk duplication, ID:", c.ID)
		//Ask for chunk size (op)
		var s *VirtualServer
		for k := range c.Holders {
			s = k
			break
		}
		if s == nil {
			log.Println("No servers available, duplication aborted, data loss?")
			return
		}
		//Check free space (OS)
		//log.Println(getFreeDiskSpace() / 1024 / 1024 / 1024)
		s.NeedConnection()
		size, err := s.Conn.GetChunkInfo(c.ID)
		if err != nil {
			log.Println("GetChunkInfo failed, duplication aborted", err)
			return
		}
		log.Println("Size", size)
		ch <- c
	}

	go func() {
		for {
			c := <-ch
			//A chunk duplication has been confirmed, transfer it now
			atomic.StoreInt64((*int64)(&sg.chunkStatus[c.ID]), 1)
			log.Println("Chunk duplication confirmed, transfering...", c.ID)
			//While transfer in course, set and get return chunk not synched
			//Ready to transfer
			//Request chunk transfer, get SYNC params
			//Set chunk as ready
		}
	}()

	return duplicate
}
