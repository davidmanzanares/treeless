package tlcom

import (
	"container/heap"
	"log"
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
	ch := make(chan *VirtualChunk)
	mutex := &sg.mutex
	mutex.Lock()
	duplicate := duplicator()
	go func() {
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
				tick = time.Tick(time.Second * 10000000)
			}
			mutex.Unlock()
			select {
			case chunk := <-ch:
				mutex.Lock()
				chunk.timeToReview = time.Now().Add(time.Millisecond * time.Duration(chunk.rank%(16*1024))) //ToDo add constant
				//log.Println("Chunk eta:", chunk.timeToReview, chunk.rank)
				if inQueue[chunk] {
					pq.update(chunk, time.Now())
				} else {
					heap.Push(&pq, chunk)
					inQueue[chunk] = true
				}
			case <-tick:
				mutex.Lock()
				c := heap.Pop(&pq).(*VirtualChunk)
				if len(c.holders) < sg.Redundancy {
					duplicate(c)
				}
				delete(inQueue, c)
				//log.Println("Checking", c.rank, c.timeToReview)
			}
		}
	}()
	return ch
}

func duplicator() (duplicate func(c *VirtualChunk)) {
	ch := make(chan *VirtualChunk, 1024)
	duplicate = func(c *VirtualChunk) {
		//Execute this code as soon as possible, adding the chunk to the known list is time critical
		log.Println("Request chunk duplication, ID:", c.id, "Rank:", c.rank)
		//Ask for chunk size (op)
		//Check free space (OS)
		//Add chunk to known list (servergroup)
		ch <- c
	}
	go func() {
		for {
			c := <-ch
			//A chunk duplication has been confirmed, transfer it now
			log.Println("Chunk duplication, confirmed:", c.id)
			//While transfer in course, set and get return chunk not synched
			//Ready to transfer
			//Request chunk transfer, get SYNC params
			//Set chunk as ready
		}
	}()
	return duplicate
}
