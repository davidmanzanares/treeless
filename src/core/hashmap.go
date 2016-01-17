package tlcore

//HashMap stores an open-addressed hashmap and all its meta-data
type HashMap struct {
	SizeLimit       uint32   //Maximum size, the map won't be expanded more than this value
	size            uint32   //Size of mem, must be a power of 2
	sizeMask        uint32   //SizeMask must have its log2(Size) least significant bits set to one
	sizelog2        uint32   //Log2(Size)
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

func newHashMap() *HashMap {
	m := new(HashMap)
	m.setSize(defaultHashMapInitialLog2Size)
	m.SizeLimit = defaultHashMapSizeLimit
	m.mem = make([]uint32, m.size*8)
	return m
}

func (m *HashMap) alloc() {
	m.setSize(defaultHashMapInitialLog2Size)
	m.mem = make([]uint32, m.size*8)
}

func (m *HashMap) setSize(log2Size uint32) {
	m.sizelog2 = log2Size
	m.size = 1 << log2Size
	m.sizeMask = 0
	for i := uint32(0); i < log2Size; i++ {
		m.sizeMask |= 1 << i
	}
	m.numKeysToExpand = uint32(float64(m.size) * defaultHashMapMaxLoadFactor)
}

func (m *HashMap) expand() error {
	panic("expand todo")
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
