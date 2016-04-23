/*
Package pmap provides persistent dicctionaries with high-performance access and specific timestamp semantics.
*/
package pmap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"time"
	"treeless/hashing"
)

//FilePerms i
const FilePerms = 0700

const defaultCheckSumInterval = time.Second

/*
A PMap is a persistent dicctionary with high-performance access and specific timestamp semantics.

A PMap has some utility functions and 4 primitives: Get, Set, Del and CAS.

Set, del and cas value parameters should contain an 8 byte long header with a timestamp.
Get returns a similar value (header + real value).
These timestamps have specific semantics described in each operation.

They are composed by a hashmap and a list:
-The hashmap is stored in memory (RAM-only). It is used to index key-value pairs.
It uses 8 bytes per bucket and it is expanded at twice its size each time a load factor is reached.
-The list is stored in a memory-mapped file, RAM vs disk usage is controlled by
kernel. It uses an 8 byte long header.

Note: this module is *not* thread-safe.
*/
type PMap struct {
	hm       *hashmap
	st       *store
	checksum syncChecksum
	path     string
}

//New returns an initialized PMap stored in path with a maximum store size.
//Set path to "" to make the PMap anonymous, it will use RAM for everything and it won't use the file system.
func New(path string, size uint64) *PMap {
	c := new(PMap)
	c.path = path
	c.hm = newHashMap(defaultHashMapInitialLog2Size, defaultHashMapSizeLimit)
	c.st = newStore(c.path, size)
	c.checksum.SetInterval(defaultCheckSumInterval)
	return c
}

func (c *PMap) Checksum() uint64 {
	c.checksum.Sum(0, time.Now())
	return c.checksum.Checksum()
}

func (c *PMap) Restore(path string) {
	c.st.open(path)
	c.hm.alloc()
	for index := uint64(0); index < c.st.length; {
		if c.st.isPresent(index) {
			key := c.st.key(index)
			val := c.st.val(index)
			h64 := hashing.FNV1a64(key)
			c.restorePair(h64, key, val, uint32(index))
		}
		index += 8 + uint64(c.st.totalLen(index))
	}
}

//Close closes a PMap. The hashmap is destroyed and the store is disk synched.
//Close will panic if it is called 2 times.
func (c *PMap) Close() {
	c.st.close()
}

//CloseAndDelete closes the PMap and removes associated files.
func (c *PMap) CloseAndDelete() {
	c.st.close()
	c.st.deleteStore()
}

//Wipe deletes
func (c *PMap) Wipe() {
	c.st.close()
	c.hm = newHashMap(defaultHashMapInitialLog2Size, defaultHashMapSizeLimit)
	c.st = newStore(c.path, c.st.size)
}

//Deleted returns the number of bytes deleted
func (c *PMap) Deleted() int {
	return int(c.st.deleted)
}

//Used retunrs the number of bytes used
func (c *PMap) Used() int {
	return int(c.st.length)
}

//Size retunrs the size of the pmap
func (c *PMap) Size() int {
	return int(c.st.size)
}

/*
	Primitives
*/

//Get returns the key's associated value or nil if it doesn't exists (or was deleted)
//If the pair doesn't exist it will return (nil, nil), non-existance is not considered an error
//The first 8 bytes contain the timestamp of the pair (nanoseconds elapsed since Unix time).
//Returned value is a copy of the stored one
func (c *PMap) Get(h32 uint32, key []byte) ([]byte, error) {
	h := uint32(h32)
	//Search for the key by using open adressing with linear probing
	index := h & c.hm.sizeMask
	for {
		storedHash := c.hm.getHash(index)
		if storedHash == emptyBucket {
			return nil, nil
		} else if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.hm.getStoreIndex(index)
			storedKey := c.st.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				v := c.st.val(uint64(stIndex))
				//We need to copy the value, returning a memory mapped file slice is dangerous,
				//the mutex wont be hold after this function returns
				vc := make([]byte, len(v))
				copy(vc, v)
				return vc, nil
			}
		}
		index = (index + 1) & c.hm.sizeMask
	}
}

//Set sets the value of a pair if the pair doesn't exists or if
//the already stored pair timestamp is before the provided timestamp.
//The first 8 bytes of value should contain the timestamp of the pair (nanoseconds elapsed since Unix time).
func (c *PMap) Set(h64 uint64, key, value []byte) error {
	//Check for available space
	if c.hm.numStoredKeys >= c.hm.numKeysToExpand {
		err := c.hm.expand()
		if err != nil {
			return err
		}
	}

	h := hashReMap(uint32(h64))
	index := h & c.hm.sizeMask
	for {
		storedHash := c.hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			storeIndex, err := c.st.put(key, value)
			if err != nil {
				return err
			}
			c.hm.setHash(index, h)
			c.hm.setStoreIndex(index, storeIndex)
			c.hm.numStoredKeys++
			t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
			c.checksum.Sum(h64^hashing.FNV1a64(value), t)
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.hm.getStoreIndex(index)
			storedKey := c.st.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				//Last write wins
				v := c.st.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
				if oldT.After(t) || oldT.Equal(t) {
					//Stored pair is newer than the provided pair
					//fmt.Println("Discarded", key, value, t)
					return nil
				}
				storeIndex, err := c.st.put(key, value)
				if err != nil {
					return err
				}
				c.checksum.Sub(h64^hashing.FNV1a64(v), t)
				c.st.del(stIndex)
				c.hm.setHash(index, h)
				c.hm.setStoreIndex(index, storeIndex)
				c.checksum.Sum(h64^hashing.FNV1a64(value), t)
				return nil
			}
		}
		index = (index + 1) & c.hm.sizeMask
	}
}

//CAS (compare and swap) sets a pair value if 2 tests are passed.
//The value should be in this format:
//[0:8]   => CAS timestamp
//[8:16]  => old value FNV1a64 hash
//[16:24] => new timestamp
//[24:]   => new value
//Tests:
//1. Stored value timstamp match the CAS timestamp, if the pair doesn't exists the CAS timestamp should be 0
//2. Stored value hash matches the provided hash
//It retunrs nil if the new value was written
func (c *PMap) CAS(h64 uint64, key, value []byte) error {
	//Check for available space
	if c.hm.numStoredKeys >= c.hm.numKeysToExpand {
		err := c.hm.expand()
		if err != nil {
			return err
		}
	}

	providedTime := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
	hv := binary.LittleEndian.Uint64(value[8:16])
	t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[16:24])))
	//fmt.Println(t.UnixNano())
	h := hashReMap(uint32(h64))
	index := h & c.hm.sizeMask
	for {
		storedHash := c.hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			if !providedTime.Equal(time.Unix(0, 0)) && hv != hashing.FNV1a64(nil) {
				return errors.New("CAS failed: empty pair: non-zero timestamp")
			}
			storeIndex, err := c.st.put(key, value[16:])
			if err != nil {
				return err
			}
			c.hm.setHash(index, h)
			c.hm.setStoreIndex(index, storeIndex)
			c.hm.numStoredKeys++
			c.checksum.Sum(h64^hashing.FNV1a64(value[16:]), t)
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.hm.getStoreIndex(index)
			storedKey := c.st.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map
				v := c.st.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				if t.Equal(oldT) {
					log.Println("Equal times!")
				}
				if oldT != providedTime {
					return errors.New("CAS failed: timestamp mismatch")
				}
				if hv != hashing.FNV1a64(v[8:]) {
					log.Println("hash mismatch!")
					return errors.New("CAS failed: hash mismatch")
				}
				c.checksum.Sub(h64^hashing.FNV1a64(v), t)
				c.st.del(stIndex)
				storeIndex, err := c.st.put(key, value[16:])
				if err != nil {
					return err
				}
				c.hm.setHash(index, h)
				c.hm.setStoreIndex(index, storeIndex)
				c.checksum.Sum(h64^hashing.FNV1a64(value[16:]), t)
				return nil
			}
		}
		index = (index + 1) & c.hm.sizeMask
	}
}

//Del marks as deleted a pair, future read instructions won't see the old value.
//However, it never frees the memory-mapped region associated with the deleted pair.
//It "leaks". The only way to free those regions is to delete the entire PMap.
func (c *PMap) Del(h64 uint64, key, value []byte) error {
	h := hashReMap(uint32(h64))

	//Search for the key by using open adressing with linear probing
	index := h & c.hm.sizeMask
	for {
		storedHash := c.hm.getHash(index)
		if storedHash == emptyBucket {
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.hm.getStoreIndex(index)
			storedKey := c.st.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map

				//Last write wins
				v := c.st.val(uint64(stIndex))
				oldT := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(value[:8])))
				if t.Before(oldT) {
					//Stored pair is newer than the provided pair
					return nil
				}
				c.checksum.Sub(h64^hashing.FNV1a64(v), t)
				c.st.del(stIndex)
				c.hm.setHash(index, deletedBucket)
				return nil
			}
		}
		index = (index + 1) & c.hm.sizeMask
	}
}

//Iterate calls foreach for each stored pair, it will stop iterating if the call returns false
func (c *PMap) Iterate(foreach func(key, value []byte) (continuE bool)) error {
	for index := uint64(0); index < c.st.length; {
		if c.st.isPresent(index) {
			key := c.st.key(index)
			val := c.st.val(index)
			kc := make([]byte, len(key))
			vc := make([]byte, len(val))
			copy(kc, key)
			copy(vc, val)
			ok := foreach(kc, vc)
			if !ok {
				break
			}
		}
		index += 8 + uint64(c.st.totalLen(index))
	}
	return nil
}

//This function is only used to restore the PMap after a DB close
func (c *PMap) restorePair(h64 uint64, key, value []byte, storeIndex uint32) error {
	h := hashReMap(uint32(h64))
	index := h & c.hm.sizeMask
	for {
		storedHash := c.hm.getHash(index)
		if storedHash == emptyBucket {
			//Empty bucket: put the pair
			c.hm.setHash(index, h)
			c.hm.setStoreIndex(index, storeIndex)
			c.hm.numStoredKeys++
			return nil
		}
		if h == storedHash {
			//Same hash: perform full key comparison
			stIndex := c.hm.getStoreIndex(index)
			storedKey := c.st.key(uint64(stIndex))
			if bytes.Equal(storedKey, key) {
				//Full match, the key was in the map: DB corrupted!
				panic("Key already in the map")
			}
		}
		index = (index + 1) & c.hm.sizeMask
	}
}
