package tlrebalance

import (
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"
	"treeless/src/tllocals"
	"treeless/src/tlsg"
)

const maxRebalanceWaitSeconds = 3

type Rebalancer struct {
	duplicatorChannel chan int
	w                 *sync.WaitGroup
}

func (r *Rebalancer) Stop() {
	r.duplicatorChannel <- (-1)
	r.w.Wait()
}

func StartRebalance(sg *tlsg.ServerGroup, lh *tllocals.LHStatus, ShouldStop func() bool) {
	//Delegate chunk downloads to the duplicator
	duplicate := duplicator(sg, lh, ShouldStop)
	release := releaser(sg, lh)
	//Constantly check for possible duplications to rebalance the servers,
	//servers should have more or less the same work
	go func() { //LoadRebalancer
		for !ShouldStop() {
			known := float64(lh.KnownChunks())
			total := float64(sg.NumChunks()) * float64(sg.Redundancy())
			avg := total / float64(sg.NumServers())
			if rand.Float32() > 0.75 { //LR-Duplicate
				for i := 0; i < sg.NumChunks(); i++ {
					if sg.NumHolders(i) < sg.Redundancy() && lh.ChunkStatus(i) == 0 {
						log.Println("Duplicate to mantain redundancy. Reason:", i, sg.NumHolders(i), sg.Redundancy())
						duplicate(i)
					}
				}
			} else if known+1 < avg*0.95 { //REB-Duplicate
				//Local server hass less work than it should
				//Try to download a random chunk
				c := int(rand.Int31n(int32(sg.NumChunks())))
				if lh.ChunkStatus(c) == 0 && sg.NumHolders(c) <= sg.Redundancy() {
					log.Println("Duplicate to rebalance. Reason:", known, avg)
					duplicate(c)
				}
			} else if known >= avg { //HR-Release
				//Local server has more work than it should
				//Locate a chunk with more redundancy than the required redundancy and *not* protected
				for _, cid := range lh.KnownChunksList() {
					if lh.ChunkStatus(cid) == tllocals.ChunkSynched && sg.NumHolders(cid) > sg.Redundancy() {
						log.Println("Release to rebalance.", cid, sg.NumHolders(cid), " Reason:", known, avg)
						release(cid)
						break
					}
				}

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

//The duplicator recieves chunkIDs and tries to download a copy from an external server
//It returns a function that should be called upon these IDs
//This function will check the avaibility of the chunk and it will begin
//an async download if the chunk is available
//It will download the chunk otherwise
//However this download will be executed in the background (i.e. in another goroutine).
//Duplicate will return inmediatly unless the channel buffer is filled
func duplicator(sg *tlsg.ServerGroup, lh *tllocals.LHStatus,
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
		length := s.GetChunkInfo(cid)
		if length == math.MaxUint64 {
			log.Println("GetChunkInfo failed, duplication aborted", s.Phy, cid)
			return
		}
		freeSpace := uint64(1000000000)
		if length > freeSpace {
			log.Println("Chunk duplication aborted, low free space. Chunk size:", length, "Free space:", freeSpace)
			return
		}

		lh.ChunkSetStatus(cid, tllocals.ChunkPresent)
		if length == 0 {
			go func() {
				//Heartbeat must be propagated before transfer initialization
				time.Sleep(time.Second * 2)
				log.Println("Duplication completed: 0 sized", s.Phy, cid)
				lh.ChunkSetStatus(cid, tllocals.ChunkSynched)
			}()
		} else {
			go func() {
				//Heartbeat must be propagated before transfer initialization
				time.Sleep(time.Second * 2)
				duplicateChannel <- cid
			}()
		}
	}

	go func() {
		for !ShouldStop() {
			cid := <-duplicateChannel

			//log.Println("Chunk duplication confirmed, transfering...", c.ID)
			//Ready to transfer: request chunk transfer, get SYNC params
			servers := sg.GetChunkHolders(cid)

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
					lh.ChunkSetStatus(cid, tllocals.ChunkSynched)
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

func releaser(sg *tlsg.ServerGroup, lh *tllocals.LHStatus) (release func(cid int)) {
	releaseChannel := make(chan int, 1024)

	release = func(cid int) {
		releaseChannel <- cid
	}
	go func() {
		for { //TODO wait condition
			c, ok := <-releaseChannel
			if !ok {
				return
			}
			//Request chunk protection?
			if lh.ChunkStatus(c) == tllocals.ChunkProtected {
				log.Println("Chunk release aborted: chunk already protected on localhost", c)
				continue
			}

			//Release repair

			//Request chunk protection
			protected := true
			for _, s := range sg.GetChunkHolders(c) {
				if s == nil || s.Phy == lh.LocalhostIPPort {
					continue
				}
				ok := s.Protect(c)
				if !ok {
					protected = false
					break
				}
			}
			if !protected {
				log.Println("Chunk release aborted: protection not stablished", c)
				continue
			}
			//Release
			lh.ChunkSetStatus(c, 0)
			log.Println("Remove completed", c)
		}
	}()

	return release
}
