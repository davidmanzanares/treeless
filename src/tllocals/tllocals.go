package tllocals

import (
	"math"
	"os"
	"strconv"
	"sync"
	"time"
	"treeless/src/tlcore"
	"treeless/src/tlhash"
)

type LHStatus struct {
	LocalhostIPPort string //Read-only from external packages
	dbpath          string
	knownChunks     int
	chunks          []*metaChunk
	mutex           sync.RWMutex //Global mutex, only some operations will use it
}

type metaChunk struct {
	core           *tlcore.Chunk
	status         ChunkStatus
	protectionTime time.Time
	sync.Mutex
}

type ChunkStatus int64

const (
	ChunkNotPresent ChunkStatus = iota
	ChunkPresent
	ChunkSynched
	ChunkProtected
)

/*
	Utils
*/

//dbpath==""  means ram-only
func NewLHStatus(dbpath string, size uint64, numChunks int,
	LocalhostIPPort string) *LHStatus {

	lh := new(LHStatus)
	lh.LocalhostIPPort = LocalhostIPPort

	if dbpath != "" {
		os.MkdirAll(dbpath+"/chunks/", tlcore.FilePerms)
	}
	lh.dbpath = dbpath
	lh.chunks = make([]*metaChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		lh.chunks[i] = new(metaChunk)
		if dbpath == "" {
			lh.chunks[i].core = tlcore.NewChunk("", size)
		} else {
			lh.chunks[i].core = tlcore.NewChunk(dbpath+"/chunks/"+strconv.Itoa(i), size)
		}
	}
	return lh
}

func (lh *LHStatus) Close() {
	for i := 0; i < len(lh.chunks); i++ {
		lh.chunks[i].Lock()
		lh.chunks[i].core.Close()
		lh.chunks[i].Unlock()
	}
}

/*
	Getters
*/
func (lh *LHStatus) KnownChunks() int {
	lh.mutex.RLock()
	n := lh.knownChunks
	lh.mutex.RUnlock()
	return n
}
func (lh *LHStatus) KnownChunksList() []int {
	lh.mutex.RLock()
	list := make([]int, 0, lh.knownChunks)
	for i := 0; i < len(lh.chunks); i++ {
		if lh.chunks[i].status != 0 {
			list = append(list, i)
		}
	}
	lh.mutex.RUnlock()
	return list
}
func (lh *LHStatus) ChunkStatus(id int) ChunkStatus {
	lh.mutex.RLock()
	s := lh.chunks[id].status
	lh.mutex.RUnlock()
	return s
}

/*
	Setters
*/

func (lh *LHStatus) ChunkSetStatus(cid int, st ChunkStatus) { //DELETE use SetSynched...
	lh.mutex.Lock()
	if lh.chunks[cid].status == 0 && st != 0 {
		lh.knownChunks++
	} else if lh.chunks[cid].status != 0 && st == 0 {
		lh.chunks[cid].core.Wipe()
		lh.knownChunks--
	}
	if st == ChunkProtected {
		t := time.Now()
		lh.chunks[cid].protectionTime = t
		go func(cid int, t time.Time) {
			time.Sleep(time.Second * 10)
			lh.mutex.Lock()
			defer lh.mutex.Unlock()
			if lh.chunks[cid].protectionTime == t {
				lh.chunks[cid].protectionTime = time.Time{}
				if lh.chunks[cid].status == ChunkProtected {
					lh.chunks[cid].status = ChunkSynched
				}
			}
		}(cid, t)
	}
	lh.chunks[cid].status = st
	lh.mutex.Unlock()
}

/*
	Primitives
*/

//Get the value for the provided key
func (lh *LHStatus) Get(key []byte) ([]byte, error) {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	v, err := lh.chunks[chunkIndex].core.Get(h, key)
	lh.chunks[chunkIndex].Unlock()
	return v, err
}

//Set the value for the provided key
func (lh *LHStatus) Set(key, value []byte) error {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	err := lh.chunks[chunkIndex].core.Set(h, key, value)
	lh.chunks[chunkIndex].Unlock()
	return err
}

//Delete the pair indexed by key
func (lh *LHStatus) Delete(key, value []byte) error {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	err := lh.chunks[chunkIndex].core.Del(h, key, value)
	lh.chunks[chunkIndex].Unlock()
	return err
}

func (lh *LHStatus) CAS(key, value []byte) error {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	err := lh.chunks[chunkIndex].core.CAS(h, key, value)
	lh.chunks[chunkIndex].Unlock()
	return err
}

//Iterate all key-value pairs of a chunk, executing foreach for each key-value pair
func (lh *LHStatus) Iterate(chunkIndex int, foreach func(key, value []byte) bool) error {
	lh.chunks[chunkIndex].Lock()
	err := lh.chunks[chunkIndex].core.Iterate(foreach)
	lh.chunks[chunkIndex].Unlock()
	return err
}

func (lh *LHStatus) LengthOfChunk(chunkIndex int) uint64 {
	lh.chunks[chunkIndex].Lock()
	defer lh.chunks[chunkIndex].Unlock()
	if lh.chunks[chunkIndex].status <= ChunkPresent {
		return math.MaxUint64
	}
	l := lh.chunks[chunkIndex].core.St.Length
	return l
}
