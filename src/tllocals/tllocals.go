package tllocals

import (
	"sync"
	"time"
)

type LHStatus struct {
	LocalhostIPPort     string        //TODO private
	chunkStatus         []ChunkStatus //Array of all DB chunks status
	chunkProtectionDate map[int]time.Time
	known               int
	mutex               sync.RWMutex
}

type ChunkStatus int64

const (
	ChunkPresent ChunkStatus = 1 + iota
	ChunkSynched
	ChunkProtected
)

/*
	Utils
*/

func NewLHStatus(numChunks int, LocalhostIPPort string) *LHStatus {
	lh := new(LHStatus)
	lh.chunkStatus = make([]ChunkStatus, numChunks)
	lh.LocalhostIPPort = LocalhostIPPort
	lh.chunkProtectionDate = make(map[int]time.Time)
	return lh
}

/*
	Getters
*/
func (lh *LHStatus) KnownChunks() int {
	lh.mutex.RLock()
	n := lh.known
	lh.mutex.RUnlock()
	return n
}
func (lh *LHStatus) KnownChunksList() []int {
	lh.mutex.RLock()
	list := make([]int, 0, lh.known)
	for i := 0; i < len(lh.chunkStatus); i++ {
		if lh.chunkStatus[i] != 0 {
			list = append(list, i)
		}
	}
	lh.mutex.RUnlock()
	return list
}
func (lh *LHStatus) ChunkStatus(id int) ChunkStatus {
	lh.mutex.RLock()
	s := lh.chunkStatus[id]
	lh.mutex.RUnlock()
	return s
}

/*
	Setters
*/

func (lh *LHStatus) ChunkSetStatus(cid int, st ChunkStatus) {
	lh.mutex.Lock()
	if lh.chunkStatus[cid] == 0 && st != 0 {
		lh.known++
	} else if lh.chunkStatus[cid] != 0 && st == 0 {
		lh.known--
	}
	if st == ChunkProtected {
		t := time.Now()
		lh.chunkProtectionDate[cid] = t
		go func(cid int, t time.Time) {
			time.Sleep(time.Second * 10)
			lh.mutex.Lock()
			defer lh.mutex.Unlock()
			if lh.chunkProtectionDate[cid] == t {
				delete(lh.chunkProtectionDate, cid)
				if lh.chunkStatus[cid] == ChunkProtected {
					lh.chunkStatus[cid] = ChunkSynched
				}
			}
		}(cid, t)
	}
	lh.chunkStatus[cid] = st
	lh.mutex.Unlock()
}
