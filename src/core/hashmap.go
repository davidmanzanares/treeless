package tlcore

import "errors"

//HashMap stores an open-addressed hashmap and all its meta-data
type HashMap struct {
	SizeLimit       uint32   //Maximum size, the map won't be expanded more than this value
	size            uint32   //Size of mem, must be a power of 2
	sizeMask        uint32   //SizeMask must have its log2(Size) least significant bits set to one
	Sizelog2        uint32   //Log2(Size)
	numKeysToExpand uint32   //Maximum number of keys until a expand operation is forced
	numStoredKeys   uint32   //Number of stored keys, included deleted, but not freed keys
	numDeletedKeys  uint32   //Number of non freed deleted keys
	mem             []uint32 //Hashmap memory
}

const defaultHashMapInitialLog2Size = 16
const defaultHashMapSizeLimit = 1024 * 1024
const defaultHashMapMaxLoadFactor = 0.7

const (
	emptyBucket   = 0
	deletedBucket = 1
)

func newHashMap(initialLog2Size, sizeLimit uint32) *HashMap {
	m := new(HashMap)
	m.setSize(initialLog2Size)
	m.SizeLimit = sizeLimit
	m.mem = make([]uint32, m.size*8)
	return m
}

func (m *HashMap) alloc() {
	m.setSize(m.Sizelog2)
	m.mem = make([]uint32, m.size*8)
}

//Sets sizelog2, size, sizeMask & numKeysToExpand
func (m *HashMap) setSize(log2Size uint32) {
	m.Sizelog2 = log2Size
	m.size = 1 << log2Size
	m.sizeMask = 0
	for i := uint32(0); i < log2Size; i++ {
		m.sizeMask |= 1 << i
	}
	m.numKeysToExpand = uint32(float64(m.size) * defaultHashMapMaxLoadFactor)
}

func (m *HashMap) expand() error {
	if m.size*2 > m.SizeLimit {
		err := errors.New("HashMap size limit reached")
		//TODO limit errors per second
		return err
	}
	newHM := newHashMap(m.Sizelog2+1, m.SizeLimit)
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

func (m *HashMap) getHash(index uint32) uint32 {
	return m.mem[2*index]
}
func (m *HashMap) getStoreIndex(index uint32) uint32 {
	return m.mem[2*index+1]
}
func (m *HashMap) setHash(index, hash uint32) {
	m.mem[2*index] = hash
}
func (m *HashMap) setStoreIndex(index, storeIndex uint32) {
	m.mem[2*index+1] = storeIndex
}

func hashReMap(h uint32) uint32 {
	//Keys 0 and 1 are used to represent special cases, remap those hashes to valid hashes
	if h < 2 {
		h += 2
	}
	return h
}
