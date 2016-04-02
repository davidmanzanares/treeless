package tlcore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"time"
	"treeless/src/tlhash"
)

/*
	This module implements DB chunks.

	A DB chunk is a database fragment. Database are divided in different fragments
	by using some of the bits of hash function.
	Each chunk access is serialized by using a read-write mutex.
	Thread contention is determined by the number of fragments in the DB.
*/

const FilePerms = 0700

//Chunk is a DB fragment
type Chunk struct {
	Hm   *HashMap
	St   *Store
	path string
}

func NewChunk(path string, size uint64) *Chunk {
	c := new(Chunk)
	c.path = path
	c.Hm = newHashMap(defaultHashMapInitialLog2Size, defaultHashMapSizeLimit)
	c.St = newStore(c.path, size)
	return c
}

func (c *Chunk) Restore(path string) {
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

func (c *Chunk) Close() {
	c.St.close()
}
func (c *Chunk) CloseAndDelete() {
	c.St.close()
	c.St.deleteStore()
}

func (c *Chunk) Wipe() {
	c.St.close()
	c.Hm = newHashMap(defaultHashMapInitialLog2Size, defaultHashMapSizeLimit)
	c.St = newStore(c.path, c.St.Size)
}

/*
	Primitives
*/

func (c *Chunk) Get(h64 uint64, key []byte) ([]byte, error) {
	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
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
				return vc, nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) Set(h64 uint64, key, value []byte) error {
	//Check for available space
	if c.Hm.numStoredKeys >= c.Hm.numKeysToExpand {
		err := c.Hm.expand()
		if err != nil {
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
				return err
			}
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
				//Full match, the key was in the map
				//Last write wins
				v := c.St.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
				if oldT.After(t) || oldT.Equal(t) {
					//Stored pair is newer than the provided pair
					//fmt.Println("Discarded", key, value, t)
					return nil
				}
				storeIndex, err := c.St.put(key, value)
				if err != nil {
					return err
				}
				c.St.del(stIndex)
				c.Hm.setHash(index, h)
				c.Hm.setStoreIndex(index, storeIndex)
				return nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) CAS(h64 uint64, key, value []byte) error {
	//Check for available space
	if c.Hm.numStoredKeys >= c.Hm.numKeysToExpand {
		err := c.Hm.expand()
		if err != nil {
			return err
		}
	}

	providedTime := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
	hv := binary.LittleEndian.Uint64(value[8:16])
	t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[16:24])))
	//fmt.Println(t.UnixNano())
	h := hashReMap(uint32(h64))
	index := h & c.Hm.sizeMask
	for {
		storedHash := c.Hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			if !providedTime.Equal(time.Unix(0, 0)) && hv != tlhash.FNV1a64(nil) {
				return errors.New("CAS failed: empty pair: non-zero timestamp")
			}
			storeIndex, err := c.St.put(key, value[16:])
			if err != nil {
				return err
			}
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
				//Full match, the key was in the map
				v := c.St.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				if t.Equal(oldT) {
					log.Println("Equal times!")
				}
				if oldT != providedTime {
					return errors.New("CAS failed: timestamp mismatch")
				}
				if hv != tlhash.FNV1a64(v[8:]) {
					log.Println("hash mismatch!")
					return errors.New("CAS failed: hash mismatch")
				}
				if t.Before(oldT) {
					log.Println("time in the past!")
					return errors.New("CAS failed: time in the past")
				}
				c.St.del(stIndex)
				storeIndex, err := c.St.put(key, value[16:])
				if err != nil {
					return err
				}
				c.Hm.setHash(index, h)
				c.Hm.setStoreIndex(index, storeIndex)
				return nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) Del(h64 uint64, key, value []byte) error {
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

				//Last write wins
				v := c.St.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
				if t.Before(oldT) {
					//Stored pair is newer than the provided pair
					return nil
				}
				c.St.del(stIndex)
				c.Hm.setHash(index, deletedBucket)
				return nil
			}
		}
		index = (index + 1) & c.Hm.sizeMask
	}
}

func (c *Chunk) Iterate(foreach func(key, value []byte) (continuE bool)) error {
	//TODO: this is a long-running function and it locks the mutex, it should release-retrieve it at some interval
	for index := uint64(0); index < c.St.Length; {
		if c.St.isPresent(index) {
			key := c.St.key(index)
			val := c.St.val(index)
			kc := make([]byte, len(key))
			vc := make([]byte, len(val))
			copy(kc, key)
			copy(vc, val)
			ok := foreach(kc, vc)
			if !ok {
				break
			}
		}
		index += 8 + uint64(c.St.totalLen(index))
	}
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
