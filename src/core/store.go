package tlcore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"launchpad.net/gommap"
)

/*
Binary structure of the Store

The store is composed of key-value pairs.
Each pair is represented this way:
	4 bytes:
		1  bit (MSB)	present?
		31 bits			key len
	4 bytes: value len
	Key len   bytes: key
	Value len bytes: value
The memory mapped file won't hold anymore information.
*/

//Store stores a list of pairs, in an *unordered* way
type Store struct {
	Deleted   uint64      //Deleted number of bytes
	Length    uint64      //Total length, index of new items
	Size      uint64      //Allocated size
	SizeLimit uint64      //Maximum size, the Store wont allocate more than this number of bytes
	osFile    *os.File    //OS mapped file located at Path
	file      gommap.MMap //Memory mapped file located at Path
}

const (
	headerKeyOffset   = 0
	headerValueOffset = 4
	headerSize        = 8
)

var mmapAdviseFlags = gommap.MADV_RANDOM

const defaultStoreSizeLimit = 1024 * 1024 * 16
const defaultStoreSize = 1024 * 4

func newStore(path string) *Store {
	var err error
	st := new(Store)
	st.Size = defaultStoreSize
	st.SizeLimit = defaultStoreSizeLimit
	st.osFile, err = os.OpenFile(path+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerms)
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
	st.file.Advise(mmapAdviseFlags)
	if err != nil {
		panic(err)
	}
	return st
}

func (st *Store) open(path string) {
	var err error
	st.osFile, err = os.OpenFile(path+".dat", os.O_RDWR, filePerms)
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
	st.file.UnsafeUnmap()
	st.osFile.Close()
}

//Expand the store instance giving more available space for items
func (st *Store) expand() (err error) {
	if st.Size == st.SizeLimit {
		return errors.New("Store size limit reached")
	}
	st.Size = st.Size * 2
	if st.Size > st.SizeLimit {
		st.Size = st.SizeLimit
	}
	//TODO: check order speed truncate vs unmap, need set benchmarks
	st.osFile.Truncate(int64(st.Size))
	st.file.UnsafeUnmap()
	st.file, err = gommap.Map(st.osFile.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	st.file.Advise(mmapAdviseFlags)
	if err != nil {
		panic(err)
	}
	return nil
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
func (st *Store) val(index uint64) []byte {
	return st.file[index+8+uint64(st.keyLen(index)) : index+headerSize+uint64(st.totalLen(index))]
}

func (st *Store) put(key, val []byte) (uint32, error) {
	size := uint64(headerSize + len(key) + len(val))
	index := st.Length
	st.Length += size
	for st.Length >= st.Size {
		err := st.expand()
		if err != nil {
			return 0, err
		}
	}
	st.setKeyLen(index, uint32(len(key)))
	st.setValLen(index, uint32(len(val)))
	copy(st.key(index), key)
	copy(st.val(index), val)
	return uint32(index), nil
}

func (st *Store) del(index uint32) error {
	st.setKeyLen(uint64(index), 0x80000000|st.keyLen(uint64(index)))
	return nil
}
