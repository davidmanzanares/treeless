package tlcore

/*
This module implements DB maps.
*/

import "os"

const defaultNumChunks = 256

//Map is a TreeLess map
type Map struct {
	ID     int                      //Id of this map, read-only
	Name   string                   //Unique identifier name of this map, read-only
	Chunks [defaultNumChunks]*Chunk //Internal use only
}

/*
	Utils functions
*/

//New returns a new Map
func newMap(id int, path, name string) (m *Map) {
	os.MkdirAll(path+"/maps/"+name+"/", filePerms)
	return &Map{ID: id, Name: name}
}

func (m *Map) restore() error {
	for i := 0; i < len(m.Chunks); i++ {
		if m.Chunks[i] != nil {
			m.Chunks[i].restore()
		}
	}
	return nil
}
func (m *Map) free() {
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i].close()
	}
}
func (m *Map) allocChunk(path string, chunkid int) error {
	m.Chunks[chunkid] = newChunk(chunkid, path, m.Name)
	return nil
}
func (m *Map) deleteChunk(chunkid int) {
	m.Chunks[chunkid].delete()
}

/*
	Primitives
*/

//Put a new key-value pair
func (m *Map) Put(key, value []byte) error {
	h := fnv1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].put(h, key, value)
}

//Get the value for the provided key
func (m *Map) Get(key []byte) ([]byte, error) {
	h := fnv1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].get(h, key)
}

//Set the value for the provided key
func (m *Map) Set(key, value []byte) error {
	h := fnv1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].set(h, key, value)
}

//Delete the pair indexed by key
func (m *Map) Delete(key []byte) error {
	h := fnv1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].del(h, key)
}

/*
	Helper functions
*/

const (
	offset32 = 2166136261
	offset64 = 14695981039346656037
	prime32  = 16777619
	prime64  = 1099511628211
)

func fnv1a64(b []byte) uint64 {
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}
