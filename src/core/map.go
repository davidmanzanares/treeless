package tlcore

/*
	This module implements DB maps-dicctionaries of key-value pairs.

	Replication and external communication is out of the scope of this package.
*/

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"treeless/src/hash"
)

const filePerms = 0700

/*
	External interface

	All external usage of tlcore should use only these functions
*/

//Map is a Treeless map, it stores key-value pairs on different fragmets (chunks)
//on the file system using memory mapped files
//An associated hashmap for fast key lookup is stored on RAM
type Map struct {
	Chunks []*Chunk
	path   string
}

//NewMap returns a new Map located at path and divided in numChunks fragments
//High values of numChunks provides low thread contention, but each chunk hash a minimum memory cost
//The numChunks values cannot be change dynamically, but "good" values resides in a wide range,
//only extreme values will have significant problems.
func NewMap(path string, numChunks int) (m *Map) {
	os.MkdirAll(path+"/chunks/", filePerms)
	m = &Map{path: path}
	m.Chunks = make([]*Chunk, numChunks)
	for i := 0; i < len(m.Chunks); i++ {
		m.Chunks[i] = newChunk(m.path + "/chunks/" + strconv.Itoa(i))
	}
	return m
}

//OpenMap loads an existing Map located at path, it will recreate
//associated hashmaps
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
	err = ioutil.WriteFile(m.path+"/meta.json", str, filePerms)
	if err != nil {
		panic(err)
	}
}

/*
	Primitives

	//The cost of these functions should be O(1) (in terms of DB size)
	//for present and non present keys.
	//Present keys will have normally one memory access and one memory mapped file access.
	//Non-present keys will have normally only one memory access.
	//Throughput will be highly affected if the memory mapped file access generates a page fault.
	//This should only happen if the DB size is bigger than the free RAM space.
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

//Iterate all key-value pairs of a chunk, executing foreach for each key-value pair
func (m *Map) Iterate(chunkID int, foreach func(key, value []byte)) error {
	return m.Chunks[chunkID].iterate(foreach)
}
