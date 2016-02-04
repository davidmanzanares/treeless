package tlcore

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)
import "bytes"

/*
	This file contains all core testing functions.
*/

const numChunks = 256

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

//Test lots of simple put & get, in a parallel way, test multi thread safety
func TestParSimpleLots(t *testing.T) {
	defer os.RemoveAll("testdb/")
	m1 := NewMap("testdb/", numChunks)
	var w sync.WaitGroup
	w.Add(27)
	for tid := 0; tid < 27; tid++ {
		go func(tid int) {
			k := make([]byte, 4)
			v := make([]byte, 4)
			for i := 0; i < 1024; i++ {
				binary.LittleEndian.PutUint32(k, uint32(i*27+tid))
				binary.LittleEndian.PutUint32(v, uint32(i*27+tid))
				m1.Set(k, v)
			}
			w.Done()
		}(tid)
	}
	w.Wait()

	k := make([]byte, 4)
	v := make([]byte, 4)
	for i := 0; i < 27*1024; i++ {
		binary.LittleEndian.PutUint32(k, uint32(i))
		binary.LittleEndian.PutUint32(v, uint32(i))
		rval, _ := m1.Get(k)
		if !bytes.Equal(v, rval) {
			t.Fatal("Error 1: value mismatch")
		}
	}

	m1.Close()
}

//Test lots of simple put & get, in a parallel way, test multi thread safety, after a map close
func TestParSimpleLotsRestore(t *testing.T) {
	defer os.RemoveAll("testdb/")
	{
		m1 := NewMap("testdb/", numChunks)
		var w sync.WaitGroup
		w.Add(27)
		for tid := 0; tid < 27; tid++ {
			go func(tid int) {
				k := make([]byte, 4)
				v := make([]byte, 4)
				for i := 0; i < 1024; i++ {
					binary.LittleEndian.PutUint32(k, uint32(i*27+tid))
					binary.LittleEndian.PutUint32(v, uint32(i*27+tid))
					m1.Set(k, v)
				}
				w.Done()
			}(tid)
		}
		w.Wait()
		m1.Close()
	}
	{
		m2 := OpenMap("testdb/")
		k := make([]byte, 4)
		v := make([]byte, 4)
		for i := 0; i < 27*1024; i++ {
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

//Test simple set
//Test simple del

//Test get, set, del

//Common functions test
func TestMix(t *testing.T) {
	metaTest(2000, 11, 129, 64)
}

//Large key size
func TestMixLargeKey(t *testing.T) {
	metaTest(2000, 130, 129, 64)
}

//Large value size
func TestMixLargeValue(t *testing.T) {
	metaTest(2000, 11, 555, 64)
}

func operate() {

}
func goOperate() {

}
func dbOperate() {

}
func checkDB() {

}

func randKVOpGenerator(maxKeySize, maxValueSize, seed, mult, offset int) func() (op int, k, v []byte) {
	r := rand.New(rand.NewSource(int64(seed)))
	base := make([]byte, 4)
	base2 := make([]byte, 4)
	return func() (op int, k, v []byte) {
		//TODO BUG quitar el menos uno
		opKeySize := r.Intn(maxKeySize-1) + 1
		opValueSize := r.Intn(maxValueSize-1) + 1
		binary.LittleEndian.PutUint32(base, uint32(r.Int31())*uint32(mult)+uint32(offset))
		binary.LittleEndian.PutUint32(base2, uint32(r.Int31())*uint32(mult)+uint32(offset))
		key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
		value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
		return 1 + r.Intn(2), key, value
	}
}

func metaTest(numOperations, maxKeySize, maxValueSize, threads int) {
	defer os.RemoveAll("testdb/")

	//Operate on built-in map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
		for i := 0; i < numOperations; i++ {
			opType, key, value := rNext()
			switch opType {
			case 1:
				goMap[string(key)] = value
			case 2:
				if _, ok := goMap[string(key)]; ok {
					delete(goMap, string(key))
					goDeletes = append(goDeletes, key)
				}
			}
		}
	}
	{
		//Operate on DB
		m1 := NewMap("testdb/", numChunks)

		var w sync.WaitGroup
		w.Add(threads)
		for core := 0; core < threads; core++ {
			go func(core int) {
				rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
				for i := 0; i < numOperations; i++ {
					opType, key, value := rNext()
					switch opType {
					case 1:
						m1.Set(key, value)
					case 2:
						m1.Delete(key)
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
		for i := 0; i < len(goDeletes); i++ {
			key := goDeletes[i]
			if _, ok := goMap[string(key)]; ok {
				//The key was set after a delete
				continue
			}
			_, err := m1.Get([]byte(key))
			if err == nil {
				panic(2)
			}
		}
		//Close DB
		m1.Close()
	}
	{
		//Restore DB
		m2 := OpenMap("testdb/")

		//Check again
		//Check map is in DB
		for key, value := range goMap {
			rval, err := m2.Get([]byte(key))
			//fmt.Println([]byte(key), value)
			if err != nil {
				panic(err)
			}
			if !bytes.Equal(rval, value) {
				panic(1)
			}
		}

		//Check deleteds aren't in DB
		testedDeletes := 0
		for i := 0; i < len(goDeletes); i++ {
			key := goDeletes[i]
			if _, ok := goMap[string(key)]; ok {
				//The key was set after a delete
				continue
			}
			_, err := m2.Get([]byte(key))
			if err == nil {
				panic(2)
			}
			testedDeletes++
		}
		fmt.Println("Tested deletes:", testedDeletes)
	}
}

//Test parallel complex get, set, del mix

//Tests operational limits: size

//Bench with diferents sizes

//Bench lots of parallel gets
func BenchmarkGet10(b *testing.B) {
	benchGet(b, 10)
}
func BenchmarkGet100(b *testing.B) {
	benchGet(b, 100)
}
func BenchmarkGet1000(b *testing.B) {
	benchGet(b, 1000)
}

func benchGet(b *testing.B, valueSize int) {
	//fmt.Println("BN", b.N)
	numKeys := 1024 * 1024
	defer os.RemoveAll("testdb/")
	m := NewMap("testdb/", numChunks)
	key := make([]byte, 4)
	value := bytes.Repeat([]byte("X"), valueSize)
	for i := 0; i < numKeys; i++ {
		binary.LittleEndian.PutUint32(key, uint32(i))
		err := m.Set(key, value)
		if err != nil {
			panic(err)
		}
	}
	gid := uint64(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 4)
		value := bytes.Repeat([]byte("X"), valueSize)
		id := uint32(atomic.AddUint64(&gid, 1))
		r := rand.New(rand.NewSource(int64(id)))
		for i := 0; pb.Next(); i++ {
			binary.LittleEndian.PutUint32(key, uint32(r.Intn(numKeys)))
			v, err := m.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			if !bytes.Equal(v, value) {
				b.Fatal("Values differ")
			}
		}
	})
	b.StopTimer()
	m.Close()
}

//Bench lots of sets

func BenchmarkSet10(b *testing.B) {
	benchSet(b, 10)
}

func BenchmarkSet100(b *testing.B) {
	benchSet(b, 100)
}

func BenchmarkSet500(b *testing.B) {
	benchSet(b, 500)
}

func benchSet(b *testing.B, valueSize int) {
	defer os.RemoveAll("testdb/")
	m := NewMap("testdb/", numChunks)
	gid := uint64(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := uint32(atomic.AddUint64(&gid, 1))
		r := rand.New(rand.NewSource(int64(id)))
		key := make([]byte, 4)
		value := bytes.Repeat([]byte("X"), valueSize)
		for i := 0; pb.Next(); i++ {
			binary.LittleEndian.PutUint32(key, uint32(4*r.Int31())+id)
			err := m.Set(key, value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	m.Close()
}
