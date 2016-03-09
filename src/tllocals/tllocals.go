package tllocals

import "sync"

type LHStatus struct {
	LocalhostIPPort string        //TODO private
	chunkStatus     []ChunkStatus //Array of all DB chunks status
	known           int
	mutex           sync.RWMutex
}

type ChunkStatus int64

const (
	ChunkPresent ChunkStatus = 1 + iota
	ChunkSynched
)

/*
	Utils
*/

func NewLHStatus(numChunks int, LocalhostIPPort string) *LHStatus {
	lh := new(LHStatus)
	lh.chunkStatus = make([]ChunkStatus, numChunks)
	lh.LocalhostIPPort = LocalhostIPPort
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
	lh.chunkStatus[cid] = st
	lh.mutex.Unlock()
}
