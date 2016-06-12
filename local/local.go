package local

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"treeless/com/protocol"
	"treeless/hashing"
	"treeless/local/pmap"
)

//Core provides an interface to access local stored chunks
type Core struct {
	LocalhostIPPort string //Read-only from external packages
	dbpath          string
	size            uint64
	knownChunks     int
	defragChannel   chan<- defragOp
	chunks          []*metaChunk
	mutex           sync.RWMutex //Global mutex, only some operations will use it
}

type metaChunk struct {
	core           *pmap.PMap
	status         ChunkStatus
	revision       int64
	protectionTime time.Time
	sync.Mutex
	defragMutex sync.Mutex
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

func getChunkPath(dbpath string, chunkID int, revision int64) string {
	if dbpath == "" {
		return ""
	}
	return fmt.Sprint(dbpath, "/chunks/", chunkID, "_rev", revision)
}

//dbpath==""  means ram-only
func NewCore(dbpath string, size uint64, numChunks int,
	LocalhostIPPort string) *Core {

	lh := new(Core)
	lh.LocalhostIPPort = LocalhostIPPort

	if dbpath != "" {
		os.MkdirAll(dbpath+"/chunks/", pmap.FilePerms)
	}
	lh.dbpath = dbpath
	lh.size = size
	lh.chunks = make([]*metaChunk, numChunks)
	for i := 0; i < len(lh.chunks); i++ {
		lh.chunks[i] = new(metaChunk)
	}
	lh.defragChannel = newDefragmenter(lh)
	return lh
}

func (lh *Core) getPathToOpen(chunkID int) string {
	if lh.dbpath == "" {
		return ""
	}
	files, _ := ioutil.ReadDir(lh.dbpath + "/chunks/")
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, fmt.Sprint(chunkID, "_rev")) {
			return lh.dbpath + "/chunks/" + name
		}
	}
	return ""
}

func (lh *Core) Open() {
	log.Println("Opening...")
	for i, c := range lh.chunks {
		c.Lock()
		path := lh.getPathToOpen(i)
		if path != "" {
			log.Println("Opening", path)
			c.core = pmap.Open(path)
			c.status = ChunkSynched
		}
		c.Unlock()
	}
	log.Println("Opening finished")
}

func (lh *Core) Close() {
	for _, c := range lh.chunks {
		c.Lock()
		if c.core != nil {
			c.core.Close()
		}
		c.Unlock()
	}
}

/*
	Getters
*/
func (lh *Core) KnownChunks() int {
	lh.mutex.RLock()
	n := lh.knownChunks
	lh.mutex.RUnlock()
	return n
}
func (lh *Core) ChunksList() []protocol.AmAliveChunk {
	lh.mutex.RLock()
	list := make([]protocol.AmAliveChunk, 0, lh.knownChunks)
	for i := 0; i < len(lh.chunks); i++ {
		if lh.chunks[i].status != 0 {
			list = append(list, protocol.AmAliveChunk{ID: i, Checksum: lh.chunks[i].core.Checksum()})
		}
	}
	lh.mutex.RUnlock()
	return list
}

func (lh *Core) ChunkStatus(id int) ChunkStatus {
	lh.mutex.RLock()
	s := lh.chunks[id].status
	lh.mutex.RUnlock()
	return s
}

/*
	Setters
*/

func (lh *Core) ChunkSetNoPresent(cid int) {
	lh.mutex.Lock()
	chunk := lh.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer lh.mutex.Unlock()
	if chunk.status != ChunkNotPresent {
		lh.chunks[cid].core.CloseAndDelete()
		lh.chunks[cid].core = nil
		lh.knownChunks--
	}
	chunk.status = ChunkNotPresent
}
func (lh *Core) ChunkSetPresent(cid int) {
	lh.mutex.Lock()
	chunk := lh.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer lh.mutex.Unlock()
	if chunk.status == ChunkNotPresent {
		lh.chunks[cid].core = pmap.New(getChunkPath(lh.dbpath, cid, 0), lh.size)
		lh.knownChunks++
	}
	chunk.status = ChunkPresent
}
func (lh *Core) ChunkSetSynched(cid int) {
	lh.mutex.Lock()
	chunk := lh.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer lh.mutex.Unlock()
	if chunk.status == ChunkNotPresent {
		lh.chunks[cid].core = pmap.New(getChunkPath(lh.dbpath, cid, 0), lh.size)
		lh.knownChunks++
	}
	chunk.status = ChunkSynched
}

func (lh *Core) ChunkSetSynchedIfNotProtected(cid int) {
	lh.mutex.Lock()
	chunk := lh.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer lh.mutex.Unlock()
	if chunk.status == ChunkNotPresent {
		lh.chunks[cid].core = pmap.New(getChunkPath(lh.dbpath, cid, 0), lh.size)
		lh.knownChunks++
	}
	if chunk.status != ChunkProtected {
		chunk.status = ChunkSynched
	}
}

func (lh *Core) ChunkSetProtected(cid int) {
	lh.mutex.Lock()
	chunk := lh.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer lh.mutex.Unlock()
	if chunk.status == ChunkNotPresent {
		lh.chunks[cid].core = pmap.New(getChunkPath(lh.dbpath, cid, 0), lh.size)
		lh.knownChunks++
	}
	t := time.Now()
	chunk.protectionTime = t
	go func(cid int, t time.Time) {
		time.Sleep(time.Second * 10)
		lh.mutex.Lock()
		defer lh.mutex.Unlock()
		chunk := lh.chunks[cid]
		chunk.Lock()
		defer chunk.Unlock()
		if chunk.protectionTime == t {
			chunk.protectionTime = time.Time{}
			if chunk.status == ChunkProtected {
				chunk.status = ChunkSynched
			}
		}
	}(cid, t)
	chunk.status = ChunkProtected
}

/*
	Primitives
*/

//Get the value for the provided key
func (lh *Core) Get(key []byte) ([]byte, error) {
	h := hashing.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	chunk := lh.chunks[chunkIndex]
	chunk.Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		lh.chunks[chunkIndex].Unlock()
		return nil, errors.New("ChunkNotPresent")
	}
	v, err := chunk.core.Get(uint32(h), key)
	chunk.Unlock()
	return v, err
}

//Set the value for the provided key
func (lh *Core) Set(key, value []byte) (err error) {
	h := hashing.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		err = errors.New("ChunkNotPresent")
	} else {
		err = lh.chunks[chunkIndex].core.Set(h, key, value)
	}
	lh.chunks[chunkIndex].Unlock()
	return err
}

//Delete the pair indexed by key
func (lh *Core) Delete(key, value []byte) error {
	h := hashing.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	chunk := lh.chunks[chunkIndex]
	chunk.Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		chunk.Unlock()
		return errors.New("ChunkNotPresent")
	}
	err := chunk.core.Del(h, key, value)
	delP := float64(chunk.core.Deleted()) / float64(chunk.core.Used())
	usedP := float64(chunk.core.Used()) / float64(chunk.core.Size())
	chunk.Unlock()
	if delP > 0.1 && usedP > 0.1 {
		lh.defragChannel <- defragOp{chunkID: chunkIndex}
	}
	return err
}

func (lh *Core) CAS(key, value []byte) error {
	h := hashing.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	lh.chunks[chunkIndex].Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		lh.chunks[chunkIndex].Unlock()
		return errors.New("ChunkNotPresent")
	}
	if lh.chunks[chunkIndex].status < ChunkSynched {
		lh.chunks[chunkIndex].Unlock()
		return errors.New("Not Synched")
	}
	err := lh.chunks[chunkIndex].core.CAS(h, key, value)
	lh.chunks[chunkIndex].Unlock()
	return err
}

//Iterate all key-value pairs of a chunk, executing foreach for each key-value pair
func (lh *Core) Iterate(chunkIndex int, foreach func(key, value []byte) bool) error {
	lh.chunks[chunkIndex].Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		lh.chunks[chunkIndex].Unlock()
		return errors.New("ChunkNotPresent")
	}
	err := lh.chunks[chunkIndex].core.Iterate(foreach)
	lh.chunks[chunkIndex].Unlock()
	return err
}

//Iterate all key-value pairs of a chunk, executing foreach for each key-value pair
func (lh *Core) BackwardsIterate(chunkIndex int, foreach func(key, value []byte) bool) error {
	lh.chunks[chunkIndex].Lock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		lh.chunks[chunkIndex].Unlock()
		return errors.New("ChunkNotPresent")
	}
	lh.chunks[chunkIndex].defragMutex.Lock()
	i := 0
	err := lh.chunks[chunkIndex].core.BackwardsIterate(func(key, value []byte) bool {
		if i%64 == 0 {
			lh.chunks[chunkIndex].Unlock()
			runtime.Gosched()
			lh.chunks[chunkIndex].Lock()
		}
		i++
		return foreach(key, value)
	})
	lh.chunks[chunkIndex].Unlock()
	lh.chunks[chunkIndex].defragMutex.Unlock()
	return err
}

func (lh *Core) LengthOfChunk(chunkIndex int) uint64 {
	lh.chunks[chunkIndex].Lock()
	defer lh.chunks[chunkIndex].Unlock()
	if lh.chunks[chunkIndex].status == ChunkNotPresent {
		return math.MaxUint64
	}
	if lh.chunks[chunkIndex].status <= ChunkPresent {
		return math.MaxUint64
	}
	l := lh.chunks[chunkIndex].core.Used()
	return uint64(l)
}
