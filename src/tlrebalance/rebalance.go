package tlrebalance

import (
	"container/heap"
	"log"
	"math/rand"
	"os"
	"syscall"
	"time"
	"treeless/src/tlcore"
	"treeless/src/tllocals"
	"treeless/src/tlsg"
)

const maxRebalanceWaitSeconds = 10
const channelUpdateBufferSize = 1024

var rebalanceWakeupPeriod = time.Second * 10

//The following priority queue is used to store chunk review candidates
//Each candidate is formed by a chunk ID and a review timestamp
//The queue is ordered by these timestamps
//Each candidate should be poped when the timestamp points to a a moment in the past

type ChunkToReview struct {
	id           int
	timeToReview time.Time
	index        int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*ChunkToReview

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
	item := x.(*ChunkToReview)
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
func (pq *PriorityQueue) update(c *ChunkToReview, t time.Time) {
	c.timeToReview = t
	heap.Fix(pq, c.index)
}

func StartRebalance(sg *tlsg.ServerGroup, lh *tllocals.LHStatus, core *tlcore.Map, ShouldStop func() bool) (chunkUpdateChannel chan int) {
	//We need a channel to get informed about chunk updates
	chunkUpdateChannel = make(chan int, channelUpdateBufferSize)
	//Delegate chunk downloads to the duplicator
	duplicate := duplicator(sg, lh, core, ShouldStop)

	//Constantly check for possible duplications to rebalance the servers,
	//servers should have more or less the same work
	go func() {
		time.Sleep(rebalanceWakeupPeriod)
		for !ShouldStop() {
			known := float64(lh.KnownChunks())
			total := float64(sg.NumChunks) * float64(sg.Redundancy)
			avg := total / float64(len(sg.Servers))
			if known+1 < avg*0.95 {
				//Local server hass less work than it should
				//Try to download a random chunk
				c := int(rand.Int31n(int32(sg.NumChunks)))
				if !lh.ChunkStatus(c).Present() {
					log.Println("Duplicate to rebalance")
					duplicate(c)
				}
			} else if known-1 > avg*1.05 {
				//Local server has more work than it should
				//Locate a chunk with more redundancy than the required redundancy and *not* protected

			}
			//We should wait a little
			//Wait more if the local server has almost an average work
			//Wait less if the local server has little work
			timetowait := 1.0 / (avg*0.95 - (known + 1))
			if timetowait <= 0.0 || timetowait > maxRebalanceWaitSeconds {
				timetowait = maxRebalanceWaitSeconds
			}
			//log.Println("Time to wait:", timetowait, "Avg: ", avg, "Known:", known)
			time.Sleep(time.Duration(float64(time.Second) * timetowait))
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
	go func() { //Simplify
		pq := make(PriorityQueue, 0)
		heap.Init(&pq)
		inQueue := make(map[int]*ChunkToReview)
		tick := time.Tick(time.Hour)
		for !ShouldStop() {
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
			select {
			case cid := <-chunkUpdateChannel:
				chunk := inQueue[cid]
				if chunk == nil {
					t := time.Now().Add(time.Millisecond * time.Duration(time.Duration(100*rand.Float32()))) //TODO add variable server k
					chunk = &ChunkToReview{id: cid, timeToReview: t}
					heap.Push(&pq, chunk)
					inQueue[cid] = chunk
				}
			case <-tick:
				if len(pq) > 0 {
					c := heap.Pop(&pq).(*ChunkToReview)
					if sg.NumHolders(c.id) < sg.Redundancy && !lh.ChunkStatus(c.id).Present() {
						duplicate(c.id)
					} else {
						//log.Println("Chunk duplication aborted: chunk not needed", c.ID, c, c.Holders[sg.localhost])
					}
					delete(inQueue, c.id)
				}
			}
		}
	}()
	return chunkUpdateChannel
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

/*
func releaser(sg *tlsg.ServerGroup, core *tlcore.Map) (release func(c *VirtualChunk)) {
	releaseChannel := make(chan *VirtualChunk, 1024)

	release = func(c *VirtualChunk) {
		releaseChannel <- c
	}
	go func() {
		sg.Lock()
		for !sg.stopped {
			sg.Unlock()
			c := <-releaseChannel
			sg.Lock()
			//Request chunk protection?
			//Release repair

			//Request chunk protection
			protected := true
			for s := range c.Holders {
				if s == sg.localhost {
					continue
				}
				sg.Unlock()
				ok := s.Protect(c.ID)
				sg.Lock()
				if !ok {
					protected = false
					break
				}
			}
			if !protected {
				continue
			}
			//Release
			sg.chunkStatus[c.ID] = 0
			log.Println("Disabling chunk...", c.ID)
			core.ChunkDisable(c.ID)
		}
		sg.Unlock()
	}()

	return release
}*/

//The duplicator recieves chunkIDs and tries to download a copy from an external server
//It returns a function that should be called upon these IDs
//This function will check the avaibility of the chunk and it will begin
//an async download if the chunk is available
//It will download the chunk otherwise
//However this download will be executed in the background (i.e. in another goroutine).
//Duplicate will return inmediatly unless the channel buffer is filled
func duplicator(sg *tlsg.ServerGroup, lh *tllocals.LHStatus, core *tlcore.Map,
	ShouldStop func() bool) (duplicate func(cid int)) {

	duplicateChannel := make(chan int, 1024)

	duplicate = func(cid int) {
		//Execute this code as soon as possible, adding the chunk to the known list is time critical
		//log.Println(time.Now().String()+"Request chunk duplication, ID:", c.ID)
		//Ask for chunk size (op)
		s := sg.GetAnyHolder(cid)
		if s == nil {
			log.Println("No servers available, duplication aborted, data loss?")
			return
		}
		//TODO: Check free space (OS)
		//log.Println(getFreeDiskSpace() / 1024 / 1024 / 1024)
		size := s.GetChunkInfo(cid)
		if size == 0 {
			log.Println("GetChunkInfo failed, duplication aborted", s.Phy, cid)
			return
		}
		freeSpace := uint64(1000000000)
		if size > freeSpace {
			log.Println("Chunk duplication aborted, low free space. Chunk size:", size, "Free space:", freeSpace)
			return
		}
		core.ChunkEnable(cid)
		lh.ChunkSetPresent(cid)
		go func() {
			time.Sleep(time.Second * 2)
			duplicateChannel <- cid
		}()
	}

	go func() {
		for !ShouldStop() {
			cid := <-duplicateChannel

			//log.Println("Chunk duplication confirmed, transfering...", c.ID)
			//Ready to transfer: request chunk transfer, get SYNC params
			servers := sg.GetChunkHolders(cid)

			log.Println("Chunk duplication began", cid)
			transferred := false
			for _, s := range servers {
				if s == nil || s.Phy == lh.LocalhostIPPort {
					continue
				}
				err := s.Transfer(lh.LocalhostIPPort, cid)
				if err != nil {
					log.Println(cid, s.Phy, err)
					log.Println("Chunk duplication aborted", cid)
					continue
				} else {
					//Set chunk as ready
					lh.ChunkSetSynched(cid)
					log.Println("Chunk duplication completed", cid)
					transferred = true
					break
				}
			}
			if !transferred {
				duplicate(cid)
			}

		}
	}()

	return duplicate
}
