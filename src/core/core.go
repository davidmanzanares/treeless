package tlcore

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

/*
	This file contains all utils functions.
*/

//DB stores a TreeLessDB
type DB struct {
	Maps       []*Map
	mapsByName map[string]*Map
	path       string
	mutex      sync.RWMutex
}

//OpGet represents a Get operation
const OpGet = 0
const OpPut = 1
const OpDel = 2
const OpSet = 3

const filePerms = 0700

//Create a new DB stored on path
func Create(path string) *DB {
	db := new(DB)
	path = path + "/"
	db.path = path
	db.Maps = make([]*Map, 0)
	db.mapsByName = make(map[string]*Map)

	err := os.MkdirAll(path+"maps/", filePerms)
	if err != nil {
		panic(err)
	}
	return db
}

//Open the DB stored in path
func Open(path string) *DB {
	db := new(DB)
	db.path = path
	db.mapsByName = make(map[string]*Map)

	f, err := ioutil.ReadFile(path + "/status")
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(f, db)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(db.Maps); i++ {
		db.mapsByName[db.Maps[i].Name] = db.Maps[i]
		db.Maps[i].restore()
	}
	return db
}

//!Falta mutex

//AllocMap adds a new map to the DB and allocates all map chunks
func (db *DB) AllocMap(name string) (*Map, error) {
	var id int
	for id = 0; id < len(db.Maps); id++ {
		if db.Maps[id] == nil {
			break
		}
	}
	m := newMap(id, db.path, name)
	if id >= len(db.Maps) {
		db.Maps = append(db.Maps, m)
	} else {
		db.Maps[id] = m
	}
	db.mapsByName[name] = m
	for i := 0; i < len(m.Chunks); i++ {
		m.allocChunk(db.path, i)
	}
	return m, nil
}

//DefineMap adds a new map to the DB, but does *not* allocates map chunks
func (db *DB) DefineMap(name string) (*Map, error) {
	var id int
	for id = 0; id < len(db.Maps); id++ {
		if db.Maps[id] == nil {
			break
		}
	}
	m := newMap(id, db.path, name)
	if id >= len(db.Maps) {
		db.Maps = append(db.Maps, m)
	} else {
		db.Maps[id] = m
	}
	db.mapsByName[name] = m
	return m, nil
}

//DelMap deletes a map from the DB
func (db *DB) DelMap(m *Map) error {
	delete(db.mapsByName, m.Name)
	db.Maps[m.ID] = nil
	for i := 0; i < len(m.Chunks); i++ {
		m.deleteChunk(i)
	}
	return nil
}

//Snapshoot the DB to path
func (*DB) Snapshoot(path string) {

}

//Close the DB
func (db *DB) Close() {
	for i := 0; i < len(db.Maps); i++ {
		db.Maps[i].free()
	}
	b, err := json.MarshalIndent(db, "", "\t")
	if err != nil {
		panic(err)
	}
	ioutil.WriteFile(db.path+"/status", b, filePerms)
}
