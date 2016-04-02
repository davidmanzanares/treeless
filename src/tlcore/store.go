package tlcore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"

	"launchpad.net/gommap"
)

//TODO: deleted pairs arent freed

/*
	A Store is a dynamically growing array stored on a memory mapped file.
	It manages additions and deletions, but it is indexed by Store ids.
	An additional data structure is needed to perform fast key-value look-ups.
*/

/*
Binary structure of the Store

The store is composed of key-value pairs.
Each pair is represented this way:
	4 bytes:
		1  bit (MSB)	is the pair present?
		31 bits			key length
	4 bytes: value len
	Key len   bytes: key
	Value len bytes: value
The memory mapped file won't hold anymore information.
*/

//Store stores a list of pairs, in an *unordered* way
type Store struct {
	Deleted uint64      //Deleted number of bytes
	Length  uint64      //Total length, index of new items
	Size    uint64      //Allocated size
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

func newStore(path string, size uint64) *Store {
	var err error
	st := new(Store)
	st.Size = size
	if path != "" {
		st.osFile, err = os.OpenFile(path+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, FilePerms)
		if err != nil {
			w, _ := os.Getwd()
			fmt.Println(w)
			panic(err)
		}
		err = st.osFile.Truncate(int64(st.Size))
		if err != nil {
			panic(err)
		}
		st.file, err = gommap.Map(st.osFile.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			panic(err)
		}
	} else {
		st.file, err = gommap.MapRegion(0, 0, int64(st.Size), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED|gommap.MAP_ANONYMOUS)
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

func (st *Store) open(path string) {
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
func (st *Store) close() {
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

func (st *Store) deleteStore() {
	if st.file != nil {
		panic("Not closed")
	}
	os.Remove(st.osFile.Name())
}

func (st *Store) isPresent(index uint64) bool {
	return (binary.LittleEndian.Uint32(st.file[index:]) & 0x80000000) == 0
}
func (st *Store) keyLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index:]) & 0x7FFFFFFF
}
func (st *Store) valLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index+headerValueOffset:])
}
func (st *Store) totalLen(index uint64) uint32 {
	return st.keyLen(index) + st.valLen(index)
}
func (st *Store) setKeyLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index:], x)
}
func (st *Store) setValLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index+headerValueOffset:], x)
}

//Returns a slice to the selected key
func (st *Store) key(index uint64) []byte {
	return st.file[index+headerSize : index+headerSize+uint64(st.keyLen(index))]
}

//Returns a slice to the selected value
func (st *Store) val(index uint64) []byte { //TODO use uint32 instead of uint64
	return st.file[index+8+uint64(st.keyLen(index)) : index+headerSize+uint64(st.totalLen(index))]
}

func (st *Store) put(key, val []byte) (uint32, error) {
	size := uint64(headerSize + len(key) + len(val))
	index := st.Length
	for st.Length+size >= st.Size {
		log.Println("Store size limit reached: denied put operation")
		return 0, errors.New("Store size limit reached: denied put operation")
	}
	st.Length += size
	st.setKeyLen(index, uint32(len(key)))
	st.setValLen(index, uint32(len(val)))
	copy(st.key(index), key)
	copy(st.val(index), val)
	return uint32(index), nil
}

func (st *Store) del(index uint32) error {
	st.Deleted += uint64(st.totalLen(uint64(index)))
	st.setKeyLen(uint64(index), 0x80000000|st.keyLen(uint64(index)))
	return nil
}
