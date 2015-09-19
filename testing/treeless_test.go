//Package testing tests the DB client and server
package testing

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"treeless/client"
	"treeless/com"
	"treeless/server"
)

const OpGet = 0
const OpPut = 1
const OpDel = 2
const OpSet = 3

const serverIP = "127.0.0.1"

var db *server.DBServer

func startServer() {
	db = server.Init()
	com.Ping(serverIP, time.Second)
}
func stopServer() {
	db.Close()
	os.RemoveAll("tmpDB/")
}

//Test just a few hard-coded operations
func TestSimple(t *testing.T) {
	//go tlserver.Init("testsDB")
	/*time.Sleep(time.Second)
	c := Init()
	Put(c, []byte("hola"), []byte("adios"))
	rval, err := Get(c, []byte("hola"))
	fmt.Println(string(rval), err)
	rval, err = Get(c, []byte("meh"))
	fmt.Println(rval, err)
	*/
}

//This test will make lots of PUT/SET/DELETE operations using a PRNG, then it will use GET operations to check the DB status
func metaTest(numOperations, maxKeySize, maxValueSize, threads int) {
	startServer()
	defer stopServer()

	//Operate on built-in map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		r := rand.New(rand.NewSource(int64(core)))
		base := make([]byte, 4)
		base2 := make([]byte, 4)
		for i := 0; i < numOperations; i++ {
			opType := 1 + r.Intn(3)
			opKeySize := r.Intn(maxKeySize-1) + 4
			opValueSize := r.Intn(maxValueSize-1) + 1
			binary.LittleEndian.PutUint32(base, uint32(i*64)+uint32(core))
			binary.LittleEndian.PutUint32(base2, uint32(i*64+core))
			key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
			value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
			//fmt.Println("gomap", opType, key, value)
			if len(value) > 128 {
				panic(opValueSize)
			}
			switch opType {
			case OpPut:
				if _, ok := goMap[string(key)]; !ok {
					goMap[string(key)] = value
				} else {
					fmt.Println(key)
					panic("repetition")
				}
			case OpDel:
				if _, ok := goMap[string(key)]; ok {
					//delete(goMap, string(key))
					//goDeletes = append(goDeletes, key)
				}
			case OpSet:
				if _, ok := goMap[string(key)]; ok {
					//goMap[string(key)] = value
				}
			}
		}
	}
	//Operate on DB
	c := treeless.CreateConnection(serverIP)

	t1 := time.Now()
	var w sync.WaitGroup
	w.Add(threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			r := rand.New(rand.NewSource(int64(core)))
			base := make([]byte, 4)
			base2 := make([]byte, 4)
			for i := 0; i < numOperations; i++ {
				opType := 1 + r.Intn(3)
				opKeySize := r.Intn(maxKeySize-1) + 4
				opValueSize := r.Intn(maxValueSize-1) + 1
				binary.LittleEndian.PutUint32(base, uint32(i*64)+uint32(core))
				binary.LittleEndian.PutUint32(base2, uint32(i*64+core))
				key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
				value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
				//fmt.Println("db   ", opType, key, value)
				switch opType {
				case OpPut:
					c.Put(key, value)
				case OpDel:
					//Delete(c, key)
				case OpSet:
					//m1.Set(key, value)
				}
			}
			w.Done()
		}(core)
	}
	w.Wait()
	fmt.Println("all done", time.Now().Sub(t1))
	//Wait
	time.Sleep(time.Second)
	//Check map is in DB
	for key, value := range goMap {
		if len(value) > 128 {
			fmt.Println(123)
		}
		rval, err := c.Get([]byte(key))
		if err != nil {
			fmt.Println(rval, "ASDASDSAD", value, len(rval), len(value))
			fmt.Println([]byte(key), value, rval)
			panic(err)
		}
		if !bytes.Equal(rval, value) {
			fmt.Println(rval, "ASDASDSAD", value, len(rval), len(value))
			panic(1)
		}
	}

	//Check deleteds aren't in DB
	fmt.Println("Tested deletes:", len(goDeletes))
	for i := 0; i < len(goDeletes); i++ {
		key := goDeletes[i]
		_, err := c.Get([]byte(key))
		if err == nil {
			panic(2)
		}
	}
	c.Close()
}

func TestCmplx(t *testing.T) {
	metaTest(10*1000, 10, 40, 10)
}

//Benchmark GET operations by issuing lots of GET operations from different goroutines.
//The DB is clean, all operations will return a "Key not present" error
func BenchmarkGetUnpopulated(b *testing.B) {
	startServer()
	defer stopServer()

	if testing.Verbose() {
		fmt.Println("\tNumKeys:", b.N)
	}
	/*c := Init()
	key := make([]byte, 4)
	lenValue := 20
	value := bytes.Repeat([]byte("X"), lenValue)
	for i := 0; i < b.N/32+1; i++ {
		binary.LittleEndian.PutUint32(key, uint32(3*i))
		binary.LittleEndian.PutUint32(value, uint32(3*i))
		err := Put(c, key, value)
		if err != nil {
			panic(err)
		}
	}*/
	gid := uint64(0)
	//time.Sleep(time.Second * 3)
	var w sync.WaitGroup
	w.Add(10000)
	b.ResetTimer()

	var connections [50]*treeless.Connection
	for j := 0; j < 50; j++ {
		c := treeless.CreateConnection(serverIP)
		connections[j] = c
		for i := 0; i < 200; i++ {
			go func(n int) {
				key := make([]byte, 4)
				id := uint32(atomic.AddUint64(&gid, 1))
				//fmt.Println(id)
				r := rand.New(rand.NewSource(int64(id)))
				for i := 0; i < n; i++ {
					binary.LittleEndian.PutUint32(key, uint32(3*r.Int31()))
					v, err := c.Get(key)
					if err == nil {
						b.Error("Key present", v)
					}
				}
				w.Done()
			}(b.N / 10000)
		}
	}
	w.Wait()
	b.StopTimer()
	for j := 0; j < 50; j++ {
		connections[j].Close()
	}
}

//Benchmark GET operations by issuing lots of GET operations from different goroutines.
//The DB will be populated, all operations will return the requested value
func BenchmarkGetPopulated2GB(b *testing.B) {

}

func BenchmarkPut64(b *testing.B) {

}

func BenchmarkPut256(b *testing.B) {

}

func BenchmarkPut2048(b *testing.B) {

}

func BenchmarkSet64(b *testing.B) {

}

func BenchmarkSet256(b *testing.B) {

}

func BenchmarkSet2048(b *testing.B) {

}

func BenchmarkDelete64(b *testing.B) {

}

func BenchmarkDelete256(b *testing.B) {

}

func BenchmarkDelete2048(b *testing.B) {

}
