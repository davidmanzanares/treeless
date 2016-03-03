package tlcore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"
	"treeless/src/hash"
)

/*
	This module implements DB chunks.

	A DB chunk is a database fragment. Database are divided in different fragments
	by using some of the bits of hash function.
	Each chunk access is serialized by using a read-write mutex.
	Thread contention is determined by the number of fragments in the DB.
*/

//Chunk is a DB fragment
type Chunk struct {
	Hm      *HashMap //TODO hm
	St      *Store
	stopped bool
	sync.Mutex
}

func newChunk(path string) *Chunk {
	c := new(Chunk)
	c.Hm = newHashMap(defaultHashMapInitialLog2Size, defaultHashMapSizeLimit)
	c.St = newStore(path)
	return c
}

func (c *Chunk) restore(path string) {
	c.Lock()
	defer c.Unlock()
	c.St.open(path)
	c.Hm.alloc()
	for index := uint64(0); index < c.St.Length; {
		if c.St.isPresent(index) {
			key := c.St.key(index)
			val := c.St.val(index)
			h64 := tlhash.FNV1a64(key)
			c.restorePair(h64, key, val, uint32(index))
		}
		index += 8 + uint64(c.St.totalLen(index))
	}
}

func (c *Chunk) close() {
	c.Lock()
	defer c.Unlock()
	c.stopped = true
	c.St.close()
}

/*
	Primitives
*/

func (c *Chunk) get(h64 uint64, key []byte) ([]byte, error) {
	c.Lock()
	if c.stopped { //TODO: pasar a Map
		c.Unlock()
		return nil, errors.New("Chunk closed")
	}

	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
			c.Unlock()
			return nil, nil
		} else if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Hm.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				v := c.St.val(uint64(stIndex))
				//We need to copy the value, returning a memory mapped file slice is dangerous,
				//the mutex wont be hold after this function returns
				vc := make([]byte, len(v))
				copy(vc, v)
				c.Unlock()
				return vc, nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) set(h64 uint64, key, value []byte) error {
	//fmt.Println("Set initiated", key, value)
	c.Lock()
	if c.stopped {
		c.Unlock()
		return errors.New("Chunk closed")
	}

	//Check for available space
	if c.Hm.numStoredKeys >= c.Hm.numKeysToExpand {
		err := c.Hm.expand()
		if err != nil {
			c.Unlock()
			return err
		}
	}

	h := hashReMap(uint32(h64))
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			storeIndex, err := c.St.put(key, value)
			if err != nil {
				c.Unlock()
				return err
			}
			c.Hm.setHash(index, h)
			c.Hm.setStoreIndex(index, storeIndex)
			c.Hm.numStoredKeys++
			c.Unlock()
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Hm.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				//Last write wins
				v := c.St.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
				if t.Before(oldT) {
					//Stored pair is newer than the provided pair
					//fmt.Println("Discarded", key, value, t)
					c.Unlock()
					return nil
				}
				//fmt.Println("Set", key, value, t)
				c.St.del(stIndex)
				storeIndex, err := c.St.put(key, value)
				if err != nil {
					c.Unlock()
					return err
				}
				c.Hm.setHash(index, h)
				c.Hm.setStoreIndex(index, storeIndex)
				c.Unlock()
				return nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) del(h64 uint64, key []byte) error {
	//TODO last writer wins
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		return errors.New("Chunk closed")
	}

	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Hm.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				c.St.del(stIndex)
				c.Hm.setHash(index, deletedBucket)
				return nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) iterate(foreach func(key, value []byte)) error {
	c.Lock()
	if c.stopped {
		c.Unlock()
		return errors.New("Chunk closed")
	}
	//TODO: this is a long-running function and it locks the mutex, it should release-retrieve it at some interval
	for index := uint64(0); index < c.St.Length; {
		if c.St.isPresent(index) {
			key := c.St.key(index)
			val := c.St.val(index)
			kc := make([]byte, len(key))
			vc := make([]byte, len(val))
			copy(kc, key)
			copy(vc, val)
			foreach(kc, vc)
		}
		index += 8 + uint64(c.St.totalLen(index))
	}
	c.Unlock()
	return nil
}

//This function is only used to restore the chunk after a DB close
func (c *Chunk) restorePair(h64 uint64, key, value []byte, storeIndex uint32) error {
	h := hashReMap(uint32(h64))
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			c.Hm.setHash(index, h)
			c.Hm.setStoreIndex(index, storeIndex)
			c.Hm.numStoredKeys++
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Hm.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map: DB corrupted!
				panic("Key already in the map")
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}
