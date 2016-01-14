package tltest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
	"treeless/src/client"
	"treeless/src/com/lowcom"
)

func TestMain(m *testing.M) {
	os.Chdir("..")
	cmd := exec.Command("go", "build", "-o", "treeless")
	cmd.Start()
	os.Exit(m.Run())
}

var id = 0

func LaunchServer(assoc string) (addr string, stop func()) {
	var cmd *exec.Cmd
	dbpath := "testDB" + fmt.Sprint(id)
	if assoc == "" {
		id = 0
		dbpath = "testDB" + fmt.Sprint(id)
		cmd = exec.Command("./treeless", "-create", "-port", fmt.Sprint(10000+id), "-dbpath", dbpath)
	} else {
		cmd = exec.Command("./treeless", "-assoc", assoc, "-port", fmt.Sprint(10000+id), "-dbpath", dbpath)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	id++
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Second)
	return string(tlLowCom.GetLocalIP()) + ":" + fmt.Sprint(10000+id-1),
		func() {
			cmd.Process.Kill()
			os.RemoveAll(dbpath)
			//fmt.Println(cmd.Path + cmd.Args[1] + cmd.Args[2] + cmd.Args[3] + cmd.Args[4] + " killed")
		}
}

//Test just a few hard-coded operations with one server - one client
func TestSimple(t *testing.T) {
	//Server set-up
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	client, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//Write operation
	err = client.Put([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}

	//Read operation
	value, err2 := client.Get([]byte("hola"))
	if err2 != nil {
		t.Fatal(err2)
	}
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}
}

func TestBasicRebalance(t *testing.T) {
	//Server set-up
	addr1, stop1 := LaunchServer("")
	//Client set-up
	client, err := tlclient.Connect(addr1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	//Write operation
	err = client.Put([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}
	//Second server set-up
	_, stop2 := LaunchServer(addr1)
	defer stop2()
	//Wait for rebalance
	time.Sleep(time.Second * 5)
	//First server shut down
	stop1()
	//Read operation
	value, err2 := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value), "Error:", err2)
	}
}

//Test lots of operations made by a single client against a single DB server
func TestCmplx1_1(t *testing.T) {
	//Server set-up
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	client, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	metaTest(client, 10*1000, 8, 8, 10, 256*256*256)
}

//Test lots of operations made by multiple clients against a single DB server
func TestCmplxN_1(t *testing.T) {
	//metaTest(10*1000, 10, 40, 10)
}

//Test lots of operations made by multiple clients against multiple DB servers
func TestCmplxN_N(t *testing.T) {
	//metaTest(10*1000, 10, 40, 10)
}

//This test will make lots of PUT/SET/DELETE operations using a PRNG, then it will use GET operations to check the DB status
func metaTest(c *tlclient.Client, numOperations, maxKeySize, maxValueSize, threads, maxKeys int) {

	//Operate on built-in map, DB will be checked against this map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		r := rand.New(rand.NewSource(int64(core)))
		base := make([]byte, 4)
		base2 := make([]byte, 4)
		for i := 0; i < numOperations; i++ {
			opType := 1 //r.Intn(3)
			opKeySize := r.Intn(maxKeySize-1) + 4
			opValueSize := r.Intn(maxValueSize-1) + 1
			binary.LittleEndian.PutUint32(base, (uint32(i*64)+uint32(core))%uint32(maxKeys))
			binary.LittleEndian.PutUint32(base2, uint32(i*64+core)%uint32(maxKeys))
			key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
			value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
			if len(value) > 128 {
				panic(opValueSize)
			}
			switch opType {
			case 1:
				//Put
				if _, ok := goMap[string(key)]; !ok {
					goMap[string(key)] = value
				} else {
					//fmt.Println(key)
					//panic("repetition")
				}
			case 2:
				//Delete
				if _, ok := goMap[string(key)]; ok {
					//delete(goMap, string(key))
					//goDeletes = append(goDeletes, key)
				}
			case 3:
				//Set
				if _, ok := goMap[string(key)]; ok {
					//goMap[string(key)] = value
				}
			}
		}
	}

	//Operate on TreelessDB
	t1 := time.Now()
	var w sync.WaitGroup
	w.Add(threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			r := rand.New(rand.NewSource(int64(core)))
			base := make([]byte, 4)
			base2 := make([]byte, 4)
			for i := 0; i < numOperations; i++ {
				opType := 1 //+ r.Intn(3)
				opKeySize := r.Intn(maxKeySize-1) + 4
				opValueSize := r.Intn(maxValueSize-1) + 1
				binary.LittleEndian.PutUint32(base, (uint32(i*64)+uint32(core))%uint32(maxKeys))
				binary.LittleEndian.PutUint32(base2, uint32(i*64+core)%uint32(maxKeys))
				key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
				value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
				//fmt.Println("db   ", opType, key, value)
				switch opType {
				case 1:
					c.Put(key, value)
				case 2:
					//Delete(c, key)
				case 3:
					//m1.Set(key, value)
				}
			}
			w.Done()
		}(core)
	}
	w.Wait()
	fmt.Println("Write phase completed in:", time.Now().Sub(t1))
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
}

//Benchmark GET operations by issuing lots of GET operations from different goroutines.
//The DB is clean, all operations will return a "Key not present" error
func BenchmarkGetUnpopulated(b *testing.B) {
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

//Benchmark a servergroup by issuing different operations from different clients
func BenchmarkMulti(b *testing.B) {

}
