package tlcore

/*
This module implements DB maps.
*/

import (
	"os"
	"strconv"
	"treeless/src/hash"
)

const filePerms = 0700

const OpGet = 0
const OpSet = 1
const OpDel = 2

//Map is a TreeLess map
type Map struct {
	Chunks []*Chunk
	path   string
}

/*
	Utils functions
*/

//New returns a new Map
func NewMap(path string, numChunks int) (m *Map) {
	os.MkdirAll(path+"/chunks/", filePerms)
	m = &Map{path: path}
	m.Chunks = make([]*Chunk, numChunks)
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i] = newChunk(m.path + "/chunks/" + strconv.Itoa(i) + ".dat")
	}
	return m
}

//TODO FIXME
func (m *Map) restore() error {
	for i := 0; i < len(m.Chunks); i++ {
		if m.Chunks[i] != nil {
			m.Chunks[i].restore()
		}
	}
	return nil
}

func (m *Map) Close() {
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i].close()
	}
}

/*
	Primitives
*/

//Get the value for the provided key
func (m *Map) Get(key []byte) ([]byte, error) {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].get(h, key)
}

//Set the value for the provided key
func (m *Map) Set(key, value []byte) error {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].set(h, key, value)
}

//Delete the pair indexed by key
func (m *Map) Delete(key []byte) error {
	h := tlhash.FNV1a64(key)
	//Opt: use AND operator
	chunkIndex := int((h >> 32) % uint64(len(m.Chunks)))
	return m.Chunks[chunkIndex].del(h, key)
}

//Iterate all key-value pairs of a chunk
func (m *Map) Iterate(chunkID int, foreach func(key, value []byte)) error {
	return m.Chunks[chunkID].iterate(foreach)
}
