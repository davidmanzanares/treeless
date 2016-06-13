package core

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
	"treeless/core/pmap"
	"treeless/hashing"
)

var protectionTime = time.Second * 10

//Core provides an interface to access local stored chunks
type Core struct {
	dbpath        string
	chunkSize     uint64
	knownChunks   int
	chunks        []*metaChunk
	defragChannel chan<- defragOp
	mutex         sync.RWMutex //Global mutex, only some operations will use it
}

type metaChunk struct {
	pm                 *pmap.PMap
	present, protected bool
	revision           int64
	protectionTime     time.Time
	sync.Mutex
	defragMutex sync.Mutex
}

//New creates a new server core instance
//dbpath is the path to store the DB, dbpath="" means RAM only
//chunkSize is the size in bytes of the chunk
//numChunks is the number of chunks of the DB
//Every chunk will be disabled (present flag = false)
func New(dbpath string, chunkSize uint64, numChunks int) *Core {
	c := new(Core)

	if dbpath != "" {
		os.MkdirAll(dbpath+"/chunks/", pmap.FilePerms)
	}
	c.dbpath = dbpath
	c.chunkSize = chunkSize
	c.chunks = make([]*metaChunk, numChunks)
	for i := 0; i < len(c.chunks); i++ {
		c.chunks[i] = new(metaChunk)
	}
	c.defragChannel = newDefragmenter(c)
	return c
}

//Returns the filesystem path given the chunk ID and its revision number
func (c *Core) chunkPath(chunkID int, revision int64) string {
	if c.dbpath == "" {
		return ""
	}
	return fmt.Sprint(c.dbpath, "/chunks/", chunkID, "_rev", revision)
}

//Finds a previous closed chunk file and returns its path, returns "" if it doesn't find anything
func (c *Core) findChunk(chunkID int) string {
	if c.dbpath == "" {
		return ""
	}
	files, _ := ioutil.ReadDir(c.dbpath + "/chunks/")
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, fmt.Sprint(chunkID, "_rev")) {
			return c.dbpath + "/chunks/" + name
		}
	}
	return ""
}

//Open will open an already stored DB, Open should be called after New
func (c *Core) Open() {
	log.Println("Opening...")
	for i, chunk := range c.chunks {
		chunk.Lock()
		path := c.findChunk(i)
		if path != "" {
			log.Println("Opening", path)
			chunk.pm = pmap.Open(path)
			chunk.present = true
		}
		chunk.Unlock()
	}
	log.Println("Opening finished")
}

//Close will close the DB, flushing all changes to disk
func (c *Core) Close() {
	for _, chunk := range c.chunks {
		chunk.Lock()
		if chunk.pm != nil {
			chunk.pm.Close()
		}
		chunk.Unlock()
	}
}

/*
	Getters
*/

//Returns the number of present chunks
func (c *Core) PresentChunks() int {
	c.mutex.RLock()
	n := c.knownChunks
	c.mutex.RUnlock()
	return n
}

//Returns the list of present chunk IDs
func (c *Core) PresentChunksList() []protocol.AmAliveChunk {
	c.mutex.RLock()
	list := make([]protocol.AmAliveChunk, 0, c.knownChunks)
	for id, chunk := range c.chunks {
		if chunk.present {
			list = append(list, protocol.AmAliveChunk{ID: id, Checksum: chunk.pm.Checksum()})
		}
	}
	c.mutex.RUnlock()
	return list
}

//IsPresent returns true if the chunk is present, false otherwise
func (c *Core) IsPresent(id int) bool {
	c.mutex.RLock()
	p := c.chunks[id].present
	c.mutex.RUnlock()
	return p
}

//IsPresent returns true if the chunk is protected, false otherwise
func (c *Core) IsProtected(id int) bool {
	c.mutex.RLock()
	p := c.chunks[id].protected
	c.mutex.RUnlock()
	return p
}

/*
	Setters
*/

//ChunkSetPresent enables the present flag of a chunk
func (c *Core) ChunkSetPresent(cid int) {
	c.mutex.Lock()
	chunk := c.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer c.mutex.Unlock()
	if !chunk.present {
		chunk.pm = pmap.New(c.chunkPath(cid, 0), c.chunkSize)
		c.knownChunks++
		chunk.present = true
	}
}

//ChunkSetNoPresent disables the present flag of a chunk
func (c *Core) ChunkSetNoPresent(cid int) {
	c.mutex.Lock()
	chunk := c.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer c.mutex.Unlock()
	if chunk.present {
		chunk.pm.CloseAndDelete()
		chunk.pm = nil
		c.knownChunks--
		chunk.present = false
		chunk.protected = false
	}
}

//ChunkSetProtected enables the protected flag of a chunk
//The flag will be disabled automatically after a period of time
func (c *Core) ChunkSetProtected(cid int) error {
	c.mutex.Lock()
	chunk := c.chunks[cid]
	chunk.Lock()
	defer chunk.Unlock()
	defer c.mutex.Unlock()
	if !chunk.present {
		return errors.New("Not present")
	}
	t := time.Now()
	chunk.protected = true
	chunk.protectionTime = t
	go func(cid int, t time.Time) {
		time.Sleep(protectionTime)
		c.mutex.Lock()
		defer c.mutex.Unlock()
		chunk := c.chunks[cid]
		chunk.Lock()
		defer chunk.Unlock()
		if chunk.protectionTime == t {
			chunk.protectionTime = time.Time{}
			chunk.protected = false
		}
	}(cid, t)
	return nil
}

/*
	Primitives
*/

//Get gets the value for the provided key
func (lh *Core) Get(key []byte) ([]byte, error) {
	h := hashing.FNV1a64(key)
	chunkIndex := int((h >> 32) % uint64(len(lh.chunks)))
	chunk := lh.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		chunk.Unlock()
		return nil, errors.New("ChunkNotPresent")
	}
	v, err := chunk.pm.Get(uint32(h), key)
	chunk.Unlock()
	return v, err
}

//Set sets the value for the provided key
func (c *Core) Set(key, value []byte) (err error) {
	h := hashing.FNV1a64(key)
	chunkIndex := int((h >> 32) % uint64(len(c.chunks)))
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		err = errors.New("ChunkNotPresent")
	} else {
		err = chunk.pm.Set(h, key, value)
	}
	chunk.Unlock()
	return err
}

//Delete deletes the pair indexed by key
func (c *Core) Delete(key, value []byte) error {
	h := hashing.FNV1a64(key)
	chunkIndex := int((h >> 32) % uint64(len(c.chunks)))
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		chunk.Unlock()
		return errors.New("ChunkNotPresent")
	}
	err := chunk.pm.Del(h, key, value)
	delP := float64(chunk.pm.Deleted()) / float64(chunk.pm.Used())
	usedP := float64(chunk.pm.Used()) / float64(chunk.pm.Size())
	chunk.Unlock()
	if delP > 0.1 && usedP > 0.1 {
		c.defragChannel <- defragOp{chunkID: chunkIndex}
	}
	return err
}

//CAS makes an Compare And Swap operation
//isSynced should return true if the chunk is synced
//value uses a special convention, see package pmap
func (c *Core) CAS(key, value []byte, isSynced func(chunkIndex int) bool) error {
	h := hashing.FNV1a64(key)
	chunkIndex := int((h >> 32) % uint64(len(c.chunks)))
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		chunk.Unlock()
		return errors.New("ChunkNotPresent")
	}
	if !isSynced(chunkIndex) {
		chunk.Unlock()
		return errors.New("ChunkNotSynced")
	}
	err := chunk.pm.CAS(h, key, value)
	chunk.Unlock()
	return err
}

//Iterate all key-value pairs of a chunk, executing foreach for each key-value pair
//it will stop early if foreach returns false
func (c *Core) Iterate(chunkIndex int, foreach func(key, value []byte) bool) error {
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		chunk.Unlock()
		return errors.New("ChunkNotPresent")
	}
	i := 0
	err := chunk.pm.Iterate(func(key, value []byte) bool {
		if i%64 == 0 {
			chunk.Unlock()
			runtime.Gosched()
			chunk.Lock()
		}
		i++
		return foreach(key, value)
	})
	chunk.Unlock()
	return err
}

//Iterate all key-value pairs of a chunk in backwards direction, executing foreach for each key-value pair
//it will stop early if foreach returns false
func (c *Core) BackwardsIterate(chunkIndex int, foreach func(key, value []byte) bool) error {
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	if !chunk.present {
		chunk.Unlock()
		return errors.New("ChunkNotPresent")
	}
	chunk.defragMutex.Lock()
	i := 0
	err := chunk.pm.BackwardsIterate(func(key, value []byte) bool {
		if i%64 == 0 {
			chunk.Unlock()
			runtime.Gosched()
			chunk.Lock()
		}
		i++
		return foreach(key, value)
	})
	chunk.Unlock()
	chunk.defragMutex.Unlock()
	return err
}

//LengthOfChunk returns the number of bytes used in the store, or math.MaxUint64 if the chunk isn't present
func (c *Core) LengthOfChunk(chunkIndex int) uint64 {
	chunk := c.chunks[chunkIndex]
	chunk.Lock()
	defer chunk.Unlock()
	if !chunk.present {
		return math.MaxUint64
	}
	return uint64(chunk.pm.Used())
}
