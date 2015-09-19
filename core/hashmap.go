package tlcore

//HashMap stores an open-addressed hashmap
type HashMap struct {
	SizeLimit       uint32 //maximum size, the map won't be expanded more than this value
	Size            uint32 //size must be a power of 2
	SizeMask        uint32 //sizeMask must have its log2Size least significant bits set to one
	Sizelog2        uint32 //log2(size)
	NumKeysToExpand uint32 //maximum number of keys until a expand operation is forced
	NumStoredKeys   uint32 //number of stored keys, included deleted, but not freed keys
	NumDeletedKeys  uint32 //number of non freed deleted keys
	mem             []uint32
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
	m.mem = make([]uint32, m.Size*8)
	return m
}

func (m *HashMap) open() {
	m.mem = make([]uint32, m.Size*8)
}

func (m *HashMap) setSize(log2Size uint32) {
	m.Sizelog2 = log2Size
	m.Size = 1 << log2Size
	m.SizeMask = 0
	for i := uint32(0); i < log2Size; i++ {
		m.SizeMask |= 1 << i
	}
	m.NumKeysToExpand = uint32(float64(m.Size) * defaultHashMapMaxLoadFactor)
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
