package tlcom

import (
	"container/heap"
	"log"
	"math/rand"
	"os"
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

func rebalancer(sg *ServerGroup) chan *VirtualChunk {
	ch := sg.ChunkUpdateChannel
	duplicate := duplicator()

	go func() {
		time.Sleep(time.Second * 100)
		sg.Lock()
		for {
			known := float64(len(sg.Localhost.HeldChunks))
			total := float64(len(sg.Chunks))
			avg := total / float64(len(sg.Servers))
			timetowait := 1.0 / (avg - known)
			if timetowait < 0.0 || timetowait > 100 {
				sg.Unlock()
				time.Sleep(time.Second * 100)
				sg.Lock()
				continue
			}
			sg.Unlock()
			time.Sleep(time.Duration(float64(time.Second) * timetowait))
			sg.Lock()
			c := &sg.Chunks[rand.Int31n(int32(len(sg.Chunks)))]
			if !c.Holders[sg.Localhost] {
				duplicate(c)
			}
		}
	}()

	go func() {
		sg.Lock()
		pq := make(PriorityQueue, 0)
		heap.Init(&pq)
		inQueue := make(map[*VirtualChunk]bool)
		tick := time.Tick(time.Second * 10000000)
		//Rebalance chunks with low redundancy
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
				chunk.timeToReview = time.Now().Add(time.Microsecond * time.Duration(60*1000*1000*rand.Float32())) //TODO add variable server k
				//log.Println("Chunk eta:", chunk.timeToReview, chunk.rank)
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
					if len(c.Holders) < sg.Redundancy && c.Holders[sg.Localhost] == false {
						duplicate(c)
					}
					delete(inQueue, c)
					//log.Println("Checking", c.rank, c.timeToReview)
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

func duplicator() (duplicate func(c *VirtualChunk)) {
	ch := make(chan *VirtualChunk, 1024)
	duplicate = func(c *VirtualChunk) {
		//Execute this code as soon as possible, adding the chunk to the known list is time critical
		log.Println(time.Now().String()+"Request chunk duplication, ID:", c.ID)
		//Ask for chunk size (op)
		var s *VirtualServer
		for k := range c.Holders {
			s = k
		}
		if s == nil {
			log.Println("No servers available, duplication aborted")
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
		//Add chunk to known list (servergroup)
		ch <- c
	}
	go func() {
		for {
			c := <-ch
			//A chunk duplication has been confirmed, transfer it now
			log.Println("Chunk duplication, confirmed:", c.ID)
			//While transfer in course, set and get return chunk not synched
			//Ready to transfer
			//Request chunk transfer, get SYNC params
			//Set chunk as ready
		}
	}()
	return duplicate
}
