package pmap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"

	"launchpad.net/gommap"
)

/*
	A store is a dynamically growing array stored on a memory mapped file.
	It manages additions and deletions, but it is indexed by store ids.
	An additional data structure is needed to perform fast key-value look-ups.

	deleted pairs are never freed.
*/

/*
Binary structure of the store

The store is composed of key-value pairs.
Each pair is represented this way:
	4 bytes:
		1  bit (MSB)	is the pair present?
		31 bits			key length
	4 bytes: value len
	Key len   bytes: key
	Value len bytes: value
Metadata is not saved on the memory-mapped file.
*/

//store stores a list of pairs, in an *unordered* way
type store struct {
	deleted uint64      //deleted number of bytes
	length  uint64      //Total length, index of new items
	size    uint64      //Allocated size, it remains constant, the store cannot expand itself
	osFile  *os.File    //OS mapped file located at Path
	file    gommap.MMap //Memory mapped file located at Path
}

const (
	headerKeyOffset   = 0
	headerValueOffset = 4
	headerSize        = 8
)

//Typical DB usage will access to random positions, this won't be true
//if it is used to store long (more bytes than the page size) pairs
var mmapAdviseFlags = gommap.MADV_RANDOM

//Creates a new Store, set path to "" to create an anonymous memory-mapped region (not FS backed)
func newStore(path string, size uint64) *store {
	var err error
	st := new(store)
	st.size = size
	if path != "" {
		st.osFile, err = os.OpenFile(path+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, FilePerms)
		if err != nil {
			w, _ := os.Getwd()
			fmt.Println(w)
			panic(err)
		}
		err = st.osFile.Truncate(int64(st.size))
		if err != nil {
			panic(err)
		}
		st.file, err = gommap.Map(st.osFile.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			panic(err)
		}
	} else {
		st.file, err = gommap.MapRegion(0, 0, int64(st.size), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED|gommap.MAP_ANONYMOUS)
		if err != nil {
			panic(err)
		}
	}
	st.file.Advise(mmapAdviseFlags)
	if err != nil {
		panic(err)
	}
	return st
}

func (st *store) open(path string) {
	var err error
	st.osFile, err = os.OpenFile(path+".dat", os.O_RDWR, FilePerms)
	if err != nil {
		panic(err)
	}
	st.file, err = gommap.Map(st.osFile.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	st.file.Advise(mmapAdviseFlags)
	if err != nil {
		panic(err)
	}
}

//Close the store unmmaping the file and syncing to disk
func (st *store) close() {
	if st.file == nil {
		panic("Already closed")
	}
	err := st.file.UnsafeUnmap()
	if err != nil {
		panic(err)
	}
	st.file = nil
	if st.osFile != nil {
		err = st.osFile.Close()
		if err != nil {
			panic(err)
		}
	}
}

//Close the store and delete associated files
func (st *store) deleteStore() {
	if st.file != nil {
		panic("Not closed")
	}
	if st.osFile != nil {
		os.Remove(st.osFile.Name())
	}
}

/*
	Store access utility functions
*/
func (st *store) keyLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index+headerKeyOffset:])
}
func (st *store) valLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index+headerValueOffset:])
}
func (st *store) totalLen(index uint64) uint32 {
	return st.keyLen(index) + st.valLen(index)
}
func (st *store) setKeyLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index:], x)
}
func (st *store) setValLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index+headerValueOffset:], x)
}

func (st *store) prev(index uint64) int64 {
	if int64(index)-4 > 0 {
		return int64(index) - 12 - int64(binary.LittleEndian.Uint32(st.file[index-4:]))
	}
	return -1
}

//Returns a slice to the selected key
func (st *store) key(index uint64) []byte {
	return st.file[index+headerSize : index+headerSize+uint64(st.keyLen(index))]
}

//Returns a slice to the selected value
func (st *store) val(index uint64) []byte { //TODO use uint32 instead of uint64
	return st.file[index+headerSize+uint64(st.keyLen(index)) : index+headerSize+uint64(st.totalLen(index))]
}

//Inserts a new pair at the end of the store, it can fail (with a returning error) if the store size limit is reached
func (st *store) put(key, val []byte) (uint32, error) {
	size := uint64(4 + 4 + 4 + len(key) + len(val))
	//Cache-alignment
	//if size <= 64 && st.length%64 >= 32 && (64-st.length%64) < size {
	//st.length += 64 - st.length%64
	//}
	for st.length+size >= st.size {
		log.Println("store size limit reached: denied put operation", st.length, st.size, size)
		return 0, errors.New("store size limit reached: denied put operation")
	}
	index := st.length
	st.length += size
	st.setKeyLen(index, uint32(len(key)))
	st.setValLen(index, uint32(len(val)))
	copy(st.key(index), key)
	copy(st.val(index), val)
	binary.LittleEndian.PutUint32(st.file[int(index)+8+len(key)+len(val):], uint32(len(key)+len(val)))
	return uint32(index), nil
}
