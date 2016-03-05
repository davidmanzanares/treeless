package tllocals

import "sync/atomic"

type LHStatus struct {
	LocalhostIPPort string
	chunkStatus     []ChunkStatus //Array of all DB chunks status
	known           int64
}

type ChunkStatus int64

const (
	chunkNotPresent ChunkStatus = 0
	chunkPresent    ChunkStatus = 1 << iota
	chunkSynched
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
	return int(atomic.LoadInt64((*int64)(&lh.known)))
}
func (lh *LHStatus) KnownChunksList() []int {
	list := make([]int, 0, lh.KnownChunks())
	for i := 0; i < len(lh.chunkStatus); i++ {
		if lh.ChunkStatus(i).Present() {
			list = append(list, i)
		}
	}
	return list
}
func (lh *LHStatus) ChunkStatus(id int) ChunkStatus {
	return ChunkStatus(atomic.LoadInt64((*int64)(&lh.chunkStatus[id])))
}

func (cs ChunkStatus) Present() bool {
	return (cs & chunkPresent) != 0
}
func (cs ChunkStatus) Synched() bool {
	return (cs & chunkSynched) != 0
}

/*
	Setters
*/

func (lh *LHStatus) ChunkSetPresent(cid int) {
	atomic.StoreInt64((*int64)(&lh.chunkStatus[cid]), int64(chunkPresent))
}
func (lh *LHStatus) ChunkSetSynched(cid int) {
	atomic.StoreInt64((*int64)(&lh.chunkStatus[cid]), int64(chunkPresent|chunkSynched))
}
