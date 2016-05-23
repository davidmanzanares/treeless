package pmap

import "errors"

/*
	These are some hashmap utility functions.

	Real primitives (Get,Set and Del) are written in pmap.go since
	these primitives needs to use this file and store.go.
*/

//hashmap stores an open-addressed hashmap and all its meta-data
type hashmap struct {
	sizeLimit       uint32   //Maximum size, the map won't be expanded more than this value
	size            uint32   //Size of mem, must be a power of 2
	sizeMask        uint32   //SizeMask must have its log2(Size) least significant bits set to one
	sizelog2        uint32   //Log2(Size)
	numKeysToExpand uint32   //Maximum number of keys until a expand operation is forced
	numStoredKeys   uint32   //Number of stored keys, included deleted, but not freed keys
	numDeletedKeys  uint32   //Number of non freed deleted keys
	mem             []uint32 //Hashmap memory
}

const defaultHashMapInitialLog2Size = 16
const defaultHashMapSizeLimit = 1024 * 1024 * 64
const defaultHashMapMaxLoadFactor = 0.7

const (
	emptyBucket   = 0
	deletedBucket = 1
)

//create a new hashmap initializating its metadata and allocating an initial memory region
func newHashMap(initialLog2Size, sizeLimit uint32) *hashmap {
	m := new(hashmap)
	m.sizeLimit = sizeLimit
	m.sizelog2 = initialLog2Size
	m.alloc()
	return m
}

func (m *hashmap) alloc() {
	m.setSize(m.sizelog2)
	m.mem = make([]uint32, m.size*8)
}

//Sets sizelog2, size, sizeMask & numKeysToExpand
func (m *hashmap) setSize(log2Size uint32) {
	m.sizelog2 = log2Size
	m.size = 1 << log2Size
	m.sizeMask = 0
	for i := uint32(0); i < log2Size; i++ {
		m.sizeMask |= 1 << i
	}
	m.numKeysToExpand = uint32(float64(m.size) * defaultHashMapMaxLoadFactor)
}

//Expand the hashmap by creating a new hashmap with twice its memory. It will copy the old data into the new hashmap.
func (m *hashmap) expand() error {
	if m.size*2 > m.sizeLimit {
		err := errors.New("HashMap size limit reached")
		return err
	}
	newHM := newHashMap(m.sizelog2+1, m.sizeLimit)
	for i := uint32(0); i < m.size; i++ {
		h := m.getHash(i)
		if h > deletedBucket {
			//Put it in the new hashmap
			storeIndex := m.getStoreIndex(i)
			index := h & newHM.sizeMask
			for {
				storedHash := newHM.getHash(index)
				if storedHash == emptyBucket {
					//Empty bucket: put the pair
					newHM.setHash(index, h)
					newHM.setStoreIndex(index, storeIndex)
					newHM.numStoredKeys++
					break
				}
				//If the hash is the same there is a collision,
				//just look in the next bucket
				index = (index + 1) & newHM.sizeMask
			}
		}
	}
	*m = *newHM
	return nil
}

/*
	Each hashmap bucket has 2 32-bit registers: the hash and the store index
*/

func (m *hashmap) getHash(index uint32) uint32 {
	return m.mem[2*index]
}
func (m *hashmap) getStoreIndex(index uint32) uint32 {
	return m.mem[2*index+1]
}
func (m *hashmap) setHash(index, hash uint32) {
	m.mem[2*index] = hash
}
func (m *hashmap) setStoreIndex(index, storeIndex uint32) {
	m.mem[2*index+1] = storeIndex
}

//Hash values 0 and 1 are used to represent special cases, remap those hashes to valid hashes
func hashReMap(h uint32) uint32 {
	if h < 2 {
		h += 2
	}
	return h
}
