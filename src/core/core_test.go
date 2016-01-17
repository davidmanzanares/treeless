package tlcore

import (
	"encoding/binary"
	"os"
	"testing"
)
import "bytes"

/*
This file contains all core testing functions.

*/

const numChunks = 8

//Test a simple put & get
func TestSimple(t *testing.T) {
	m := NewMap("testdb/", numChunks)
	defer os.RemoveAll("testdb/")
	defer m.Close()

	m.Set([]byte("k1"), []byte("v1"))
	rval, _ := m.Get([]byte("k1"))
	if !bytes.Equal([]byte("v1"), rval) {
		t.Fatal("Error: value mismatch")
	}
}

//Test a simple put & get (after a map close())
func TestSimpleRestore(t *testing.T) {
	defer os.RemoveAll("testdb/")
	{
		m := NewMap("testdb/", numChunks)
		m.Set([]byte("k1"), []byte("v1"))
		m.Close()
	}
	{
		m := OpenMap("testdb/")
		rval, _ := m.Get([]byte("k1"))
		if !bytes.Equal([]byte("v1"), rval) {
			t.Fatal("Error: value mismatch")
		}
		m.Close()
	}
}

//Test lots of simple put & get before and after closing the map
func TestSimpleLots(t *testing.T) {
	defer os.RemoveAll("testdb/")
	k := make([]byte, 4)
	v := make([]byte, 4)
	{
		m1 := NewMap("testdb/", numChunks)
		for i := 0; i < 128*1024; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			binary.LittleEndian.PutUint32(v, uint32(i))
			m1.Set(k, v)
		}
		for i := 0; i < 128*1024; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			binary.LittleEndian.PutUint32(v, uint32(i))
			rval, _ := m1.Get(k)
			if !bytes.Equal(v, rval) {
				t.Fatal("Error 1: value mismatch")
			}
		}
		m1.Close()
	}
	{
		m2 := OpenMap("testdb/")
		for i := 0; i < 128*1024; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			binary.LittleEndian.PutUint32(v, uint32(i))
			rval, _ := m2.Get(k)
			if !bytes.Equal(v, rval) {
				t.Fatal("Error 2: value mismatch")
			}
		}
		m2.Close()
	}
}

/*
//Test lots of simple put & get, in a parallel way, test multi thread safety
func TestParSimpleLots(t *testing.T) {
	defer os.RemoveAll("testdb/")
	defer os.Chdir("../")
	k := make([]byte, 4)
	v := make([]byte, 4)
	{
		db := Create("testdb")
		m1, _ := db.AllocMap("mapa1")

		for tid := 0; tid < 27; tid++ {
			for i := 0; i < 1024; i++ {
				binary.LittleEndian.PutUint32(k, uint32(i*27+tid))
				binary.LittleEndian.PutUint32(v, uint32(i*27+tid))
				m1.Put(k, v)
			}
		}

		for i := 0; i < 27*1024; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			binary.LittleEndian.PutUint32(v, uint32(i))
			rval, _ := m1.Get(k)
			if !bytes.Equal(v, rval) {
				t.Fatal("Err 1: mismatch")
			}
		}

		db.Close()
	}
	{
		db := Open("testdb")
		m1 := db.mapsByName["mapa1"]
		for i := 0; i < 27*1024; i++ {
			binary.LittleEndian.PutUint32(k, uint32(i))
			binary.LittleEndian.PutUint32(v, uint32(i))
			rval, _ := m1.Get(k)
			if !bytes.Equal(v, rval) {
				t.Fatal("Err 2: mismatch")
			}
		}
		db.Close()
	}
}

//Test simple set
//Test simple del

//Test complex get, put, set, del mix

//Common functions test
func TestCmplx1(t *testing.T) {
	coreTest(100000, 11, 129, 64)
}

//Low key size: test delete operation
func TestCmplx2(t *testing.T) {
	coreTest(100000, 2, 129, 64)
}

//Large key size
func TestCmplx3(t *testing.T) {
	coreTest(10000, 130, 129, 64)
}

//Large value size
func TestCmplx4(t *testing.T) {
	coreTest(10000, 11, 555, 64)
}

//Test low value size
func TestCmplx5(t *testing.T) {
	coreTest(10000, 11, 2, 64)
}

func operate() {

}
func goOperate() {

}
func dbOperate() {

}
func checkDB() {

}

func coreTest(numOperations, maxKeySize, maxValueSize, threads int) {
	//Operate on built-in map

	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		r := rand.New(rand.NewSource(int64(core)))
		base := make([]byte, 4)
		base2 := make([]byte, 4)
		for i := 0; i < numOperations; i++ {
			opType := 1 + r.Intn(3)
			opKeySize := r.Intn(maxKeySize-1) + 1
			opValueSize := r.Intn(maxValueSize-1) + 1
			binary.LittleEndian.PutUint32(base, uint32(r.Int31()*64)+uint32(core))
			binary.LittleEndian.PutUint32(base2, uint32(i*64+core))
			key := bytes.Repeat([]byte(base), opKeySize)
			value := bytes.Repeat([]byte(base), opValueSize)
			//fmt.Println("gomap", opType, key, value)
			switch opType {
			case OpPut:
				if _, ok := goMap[string(key)]; !ok {
					goMap[string(key)] = value
				}
			case OpDel:
				if _, ok := goMap[string(key)]; ok {
					delete(goMap, string(key))
					goDeletes = append(goDeletes, key)
				}
			case OpSet:
				if _, ok := goMap[string(key)]; ok {
					goMap[string(key)] = value
				}
			}
		}
	}
	//Operate on DB
	db := Create("testdb2")
	defer os.RemoveAll("testdb2/")
	defer os.Chdir("../")

	m1, _ := db.AllocMap("mapa1")

	var w sync.WaitGroup
	w.Add(threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			r := rand.New(rand.NewSource(int64(core)))
			base := make([]byte, 4)
			base2 := make([]byte, 4)
			for i := 0; i < numOperations; i++ {
				opType := 1 + r.Intn(3)
				opKeySize := r.Intn(maxKeySize-1) + 1
				opValueSize := r.Intn(maxValueSize-1) + 1
				binary.LittleEndian.PutUint32(base, uint32(r.Int31()*64)+uint32(core))
				binary.LittleEndian.PutUint32(base2, uint32(i*64+core))
				key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
				value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
				//fmt.Println("db   ", opType, key, value)
				switch opType {
				case OpPut:
					m1.Put(key, value)
				case OpDel:
					m1.Delete(key)
				case OpSet:
					m1.Set(key, value)
				}
			}
			w.Done()
		}(core)
	}
	w.Wait()
	//Check map is in DB
	for key, value := range goMap {
		rval, err := m1.Get([]byte(key))
		//fmt.Println([]byte(key), value)
		if err != nil {
			panic(err)
		}
		if !bytes.Equal(rval, value) {
			panic(1)
		}
	}

	//Check deleteds aren't in DB
	fmt.Println("Tested deletes:", len(goDeletes))
	for i := 0; i < len(goDeletes); i++ {
		key := goDeletes[i]
		_, err := m1.Get([]byte(key))
		if err == nil {
			panic(2)
		}
	}
	//Close DB
	db.Close()

	{
		//Restore DB
		db := Open("testdb2")
		m1 := db.mapsByName["mapa1"]
		//Check again
		//Check map is in DB
		for key, value := range goMap {
			rval, err := m1.Get([]byte(key))
			//fmt.Println([]byte(key), value)
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(rval, value) {
				panic(1)
			}
		}

		//Check deleteds aren't in DB
		fmt.Println("Tested deletes:", len(goDeletes))
		for i := 0; i < len(goDeletes); i++ {
			key := goDeletes[i]
			_, err := m1.Get([]byte(key))
			if err == nil {
				panic(2)
			}
		}
	}
}

//Test parallel complex get, put, set, del mix

//Tests operational limits: size

//Test sync/nosync file

//Bench with diferents sizes

//Bench lots of gets
func BenchmarkGet(b *testing.B) {
	defer os.RemoveAll("benchdb/")
	defer os.Chdir("../")
	if testing.Verbose() {
		fmt.Println("\tInserting", b.N, "keys...")
	}
	db := Create("benchdb/")
	m, _ := db.AllocMap("mapA")
	key := make([]byte, 4)
	lenValue := 100
	value := bytes.Repeat([]byte("X"), lenValue)
	for i := 0; i < b.N/32+1; i++ {
		binary.LittleEndian.PutUint32(key, uint32(3*i))
		binary.LittleEndian.PutUint32(value, uint32(3*i))
		err := m.Put(key, value)
		if err != nil {
			panic(err)
		}
	}
	gid := uint64(0)
	fmt.Println("get...")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		sum := 0
		key := make([]byte, 4)
		id := uint32(atomic.AddUint64(&gid, 1))
		fmt.Println(id)
		r := rand.New(rand.NewSource(int64(id)))
		for i := 0; pb.Next(); i++ {
			binary.LittleEndian.PutUint32(key, uint32(3*r.Intn(b.N/32+1)))
			v, err := m.Get(key)
			sum += int(v[len(v)-1])
			if err != nil || !bytes.Equal(v, v) {
				b.Fatal("Key not present", key, id, i, v, sum)
			}
		}
	})
	b.StopTimer()
	db.Close()
}

//Bench lots of gets, in a parallel way

//Bench lots of puts

//Bench lots of puts, in a parallel way
*/
