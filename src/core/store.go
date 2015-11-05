package tlcore

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
)

//Store stores a list of pairs, *unordered*
type Store struct {
	Path      string
	Deleted   uint64    //Deleted bytes
	Length    uint64    //Total length, index of new items
	Size      uint64    //Allocated size
	SizeLimit uint64    //Maximum size
	file      mmap.MMap //Memory mapped file
	osFile    *os.File  //OS mapped file
}

const itemHeaderSize = 8
const defaultStoreSizeLimit = 1024 * 1024 * 2
const defaultStoreSize = 1024 * 1024 * 32

func newStore(path string) *Store {
	var err error
	st := new(Store)
	st.Path = path
	st.Size = defaultStoreSize
	st.SizeLimit = defaultStoreSizeLimit
	st.osFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerms)
	if err != nil {
		w, _ := os.Getwd()
		fmt.Println(w)
		panic(err)
	}
	err = st.osFile.Truncate(int64(st.Size))
	if err != nil {
		panic(err)
	}
	st.file, err = mmap.Map(st.osFile, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	return st
}

func (st *Store) open() {
	var err error
	st.osFile, err = os.OpenFile(st.Path, os.O_RDWR, filePerms)
	if err != nil {
		panic(err)
	}
	st.file, err = mmap.Map(st.osFile, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
}
func (st *Store) close() {
	st.file.Unmap()
	st.osFile.Close()
}
func (st *Store) delete() {
	st.close()
	os.Remove(st.Path)
}

func (st *Store) isPresent(index uint64) bool {
	return (binary.LittleEndian.Uint32(st.file[index:]) & 0x80000000) == 0
}
func (st *Store) keyLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index:]) & 0x7FFFFFFF
}
func (st *Store) valLen(index uint64) uint32 {
	return binary.LittleEndian.Uint32(st.file[index+4:])
}
func (st *Store) totalLen(index uint64) uint32 {
	return st.keyLen(index) + st.valLen(index)
}
func (st *Store) setKeyLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index:], x)
}
func (st *Store) setValLen(index uint64, x uint32) {
	binary.LittleEndian.PutUint32(st.file[index+4:], x)
}

//Returns a slice to the selected key
func (st *Store) key(index uint64) []byte {
	return st.file[index+8 : index+8+uint64(st.keyLen(index))]
}

//Returns a slice to the selected value
func (st *Store) val(index uint64) []byte {
	return st.file[index+8+uint64(st.keyLen(index)) : index+8+uint64(st.totalLen(index))]
}

func (st *Store) put(key, val []byte) (uint32, error) {
	size := uint64(itemHeaderSize + len(key) + len(val))
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

//Expand the store instance giving more available space for items
func (st *Store) expand() (err error) {
	panic("St expand")
	return nil
}
