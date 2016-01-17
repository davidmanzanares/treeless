package tlcore

/*
This module implements DB maps.
*/

import (
	"encoding/json"
	"io/ioutil"
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

//NewMap returns a new Map located at path and divided in numChunks fragments
func NewMap(path string, numChunks int) (m *Map) {
	os.MkdirAll(path+"/chunks/", filePerms)
	m = &Map{path: path}
	m.Chunks = make([]*Chunk, numChunks)
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i] = newChunk(m.path + "/chunks/" + strconv.Itoa(i))
	}
	return m
}

//OpenMap loads an existing map located at path
func OpenMap(path string) *Map {
	str, err := ioutil.ReadFile(path + "meta.json")
	if err != nil {
		panic(err)
	}
	m := &Map{path: path}
	err = json.Unmarshal(str, m)
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i].restore(m.path + "/chunks/" + strconv.Itoa(i))
	}
	return m
}

//Close the map writting all changes to the file system
func (m *Map) Close() {
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i].close()
	}
	str, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile(m.path+"meta.json", str, filePerms)
	if err != nil {
		panic(err)
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
