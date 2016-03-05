package tlhash

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

func FNV1a64(b []byte) uint64 {
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

func GetChunkID(b []byte, numChunks int) int {
	h := FNV1a64(b)
	return int((h >> 32) % uint64(numChunks))
}
