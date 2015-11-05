package tlcore

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
)

/*
This module implements DB chunks.
*/

//Chunk is a DB chunk
type Chunk struct {
	ID    int
	Map   *HashMap
	St    *Store
	mutex sync.RWMutex
}

func newChunk(ID int, path, mapname string) *Chunk {
	c := new(Chunk)
	c.ID = ID
	c.Map = newHashMap()
	c.St = newStore(path + "/maps/" + mapname + "/" + strconv.Itoa(ID) + ".dat")
	return c
}

func (c *Chunk) restore() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.St.open()
	c.Map.open()
	//TODO restore keys
	for index := uint64(0); index < c.St.Length; {
		if c.St.isPresent(index) {
			key := c.St.key(index)
			val := c.St.val(index)
			h64 := fnv1a64(key)
			c.keyRestore(h64, key, val, uint32(index))
		}
		index += 8 + uint64(c.St.totalLen(index))
	}
}

func (c *Chunk) delete() {
	c.St.delete()
}

func (c *Chunk) close() {
	c.St.close()
}

/*
	Primitives
*/

func (c *Chunk) put(h64 uint64, key, value []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	h := hashReMap(uint32(h64))

	//Check for available space
	if c.Map.NumStoredKeys >= c.Map.NumKeysToExpand {
		error := c.Map.expand()
		if error != nil {
			return error
		}
	}

	index := h & c.Map.SizeMask
	for {
		storedHash := c.Map.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			storeIndex, err := c.St.put(key, value)
			if err != nil {
				return err
			}
			c.Map.setHash(index, h)
			c.Map.setStoreIndex(index, storeIndex)
			c.Map.NumStoredKeys++
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Map.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				return errors.New("Key already preset")
			}
		}
		index = (index + 1) & c.Map.SizeMask
	}
}

func (c *Chunk) get(h64 uint64, key []byte) ([]byte, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Map.SizeMask
	for {
		storedHash := c.Map.getHash(index)
		if storedHash == emptyBucket {
			return nil, errors.New("Key not present")
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Map.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				return c.St.val(uint64(stIndex)), nil
			}
		}
		index = (index + 1) & c.Map.SizeMask
	}
}

func (c *Chunk) del(h64 uint64, key []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Map.SizeMask
	for {
		storedHash := c.Map.getHash(index)
		if storedHash == emptyBucket {
			return errors.New("Key not present")
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Map.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				c.St.del(stIndex)
				c.Map.setHash(index, deletedBucket)
				return nil
			}
		}
		index = (index + 1) & c.Map.SizeMask
	}
}

func (c *Chunk) set(h64 uint64, key, value []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Map.SizeMask
	for {
		storedHash := c.Map.getHash(index)
		if storedHash == emptyBucket {
			return errors.New("Key not present")
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Map.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				c.St.del(stIndex)
				storeIndex, err := c.St.put(key, value)
				if err != nil {
					return err
				}
				c.Map.setStoreIndex(index, storeIndex)
				return nil
			}
		}
		index = (index + 1) & c.Map.SizeMask
	}
}

func (c *Chunk) keyRestore(h64 uint64, key, value []byte, storeIndex uint32) error {
	h := hashReMap(uint32(h64))

	//Check for available space
	if c.Map.NumStoredKeys >= c.Map.NumKeysToExpand {
		error := c.Map.expand()
		if error != nil {
			return error
		}
	}

	index := h & c.Map.SizeMask
	for {
		storedHash := c.Map.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			c.Map.setHash(index, h)
			c.Map.setStoreIndex(index, storeIndex)
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.Map.getStoreIndex(index)
			storedKey := c.St.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				return errors.New("Key already preset")
			}
		}
		index = (index + 1) & c.Map.SizeMask
	}
}
