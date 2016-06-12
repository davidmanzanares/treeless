package test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"
	"treeless/client"
	"treeless/tlfmt"
)

const testingNumChunks = 8
const benchmarkingNumChunks = 64

var cluster []testServer
var ultraverbose = false

func TestMain(m *testing.M) {
	//debug.SetTraceback("all")
	cmd := exec.Command("killall", "treeless")
	cmd.Run()
	os.Chdir("..")
	cmd = exec.Command("go", "build", "-o", "treeless")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	cmd = exec.Command("cp", "treeless", "testing/")
	cmd.Run()
	os.Chdir("testing")
	if err != nil {
		panic("Errors building the program, testing aborted.")
	}
	flag.BoolVar(&ultraverbose, "vv", false, "Sets ultra-verbose mode")
	flag.Parse()
	if !ultraverbose {
		log.SetOutput(ioutil.Discard)
	}
	//CLUSTER INITIALIZATION
	cluster = procStartCluster(2)
	code := m.Run()
	/*for _, s := range cluster {
		s.kill()
	}*/
	os.Exit(code)
}

//Test just a few hard-coded operations with one server - one client
func TestSingleSimple(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)
	//Client set-up
	client, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//Set operation
	_, err = client.Set([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}

	//Get operation
	value, _, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	//Del operation
	err = client.Del([]byte("hola"))
	if err != nil {
		t.Fatal(err)
	}

	//Get operation
	value, _, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get returned string: ", string(value))
	}
}

//Test just a few hard-coded operations to test the persistence
func TestSingleOpen(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)
	//Client set-up
	client, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//Set operation
	_, err = client.Set([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}

	cluster[0].close()
	addr = cluster[0].create(testingNumChunks, 2, ultraverbose, true)

	//Get operation
	value, _, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	//Del operation
	err = client.Del([]byte("hola"))
	if err != nil {
		t.Fatal(err)
	}
	cluster[0].close()
	addr = cluster[0].create(testingNumChunks, 2, ultraverbose, true)

	//Get operation
	value, _, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get 2 returned string: ", string(value))
	}
}

//TestBigMessages, send 1MB GET, SET messages
func TestSingleBigMessages(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)

	//Client set-up
	client, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.SetTimeout = time.Second
	client.GetTimeout = time.Second

	//SET
	_, err = client.Set([]byte("hola"), bytes.Repeat([]byte("X"), 1024*1024))
	if err != nil {
		t.Fatal(err)
	}

	//GET
	value, _, _ := client.Get([]byte("hola"))
	if string(value) != string(bytes.Repeat([]byte("X"), 1024*1024)) {
		t.Fatal("Get failed, returned string: ", string(value))
	}
}

//TestBigMessages, send 128MB GET, SET messages, server should deny the operation
func TestSingleSizeLimit(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)

	//Client set-up
	client, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.SetTimeout = time.Second * 5
	client.GetTimeout = time.Second * 5

	//SET
	{
		_, err = client.Set([]byte("hola"), bytes.Repeat([]byte("X"), 128*1024*1024))
		//fmt.Println(err)
	}
	/*runtime.GC()
	debug.FreeOSMemory()
	var stats debug.GCStats
	debug.ReadGCStats(&stats)
	fmt.Println(stats)
	time.Sleep(time.Minute)*/
	if err == nil {
		t.Fatal("Size limit not reached")
	}

	//GET
	value, _, _ := client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get returned:", value)
	}

	//Test 1MB, these should work
	//SET
	_, err = client.Set([]byte("hola"), bytes.Repeat([]byte("X"), 1024*1024))
	if err != nil {
		t.Fatal(err)
	}

	//GET
	value, _, _ = client.Get([]byte("hola"))
	if string(value) != string(bytes.Repeat([]byte("X"), 1024*1024)) {
		t.Fatal("Get failed, returned string: ", string(value))
	}
}

//Test just a few hard-coded operations with one server - one client
func TestSingleTimeout(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	waitForServer(addr)
	//Client set-up
	client, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//Set operation
	_, err = client.Set([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}

	//Get operation
	value, _, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	cluster[0].kill()

	time.Sleep(time.Millisecond * 100)

	//Get operation
	tb := time.Now()
	client.GetTimeout = time.Millisecond * 100
	value, _, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("???", value)
	}
	log.Println("Timeout:", time.Now().Sub(tb))
}

//Test lots of operations made by a single client against a single DB server
func TestSingleCmplx1_1(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)
	metaTest(t, addr, 10*1000, 4, 8, 10, 1024)
}

//This test will make lots of PUT/SET/DELETE operations using a PRNG, then it will use GET operations to check the DB status
func metaTest(t *testing.T, addr string, numOperations, maxKeySize, maxValueSize, threads, maxKeys int) {
	runtime.GOMAXPROCS(threads)
	//Operate on built-in map, DB will be checked against this map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		rNext := randKVOpGenerator(1, maxKeySize, maxValueSize, core, 64, core)
		for i := 0; i < numOperations; i++ {
			opType, key, value := rNext()
			switch opType {
			case 0:
				//Put
				goMap[string(key)] = value
			case 1:
				//Delete
				delete(goMap, string(key))
				goDeletes = append(goDeletes, key)
			}
		}
	}

	//Operate on TreelessDB
	t1 := time.Now()
	var w sync.WaitGroup
	w.Add(threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			//Client set-up
			c, err := client.Connect(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			rNext := randKVOpGenerator(1, maxKeySize, maxValueSize, core, 64, core)
			for i := 0; i < numOperations; i++ {
				opType, key, value := rNext()
				switch opType {
				case 0:
					c.Set(key, value)
				case 1:
					c.Del(key)
				}
			}
			w.Done()
		}(core)
	}
	w.Wait()
	if testing.Verbose() {
		fmt.Println("Write phase completed in:", time.Now().Sub(t1))
	}
	//Check map is in DB
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for key, value := range goMap {
		if len(value) > 128 {
			fmt.Println(123)
		}
		rval, _, _ := c.Get([]byte(key))
		if !bytes.Equal(rval, value) {
			fmt.Println(rval, "ASDASDSAD", value, len(rval), len(value))
			panic(1)
		}
	}

	//Check deleteds aren't in DB
	dels := 0
	for i := 0; i < len(goDeletes); i++ {
		key := goDeletes[i]
		_, ok := goMap[string(key)]
		if ok {
			continue
		}
		v, _, _ := c.Get([]byte(key))
		dels++
		if v != nil {
			t.Fatal("Deleted key present on DB")
		}
	}
	if testing.Verbose() {
		fmt.Println("Present keys tested:", len(goMap))
		fmt.Println("Deleted keys tested:", dels)
	}
}

func TestSingleConsistency(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)
	metaTestConsistency(t, addr, 20, 200)
}

func TestSingleConsistencyAsyncSet(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	waitForServer(addr)
	metaTestConsistencyAsyncSet(t, addr, 20, 200)
}

func metaTestConsistencyAsyncSet(t *testing.T, serverAddr string, numClients, iterations int) {
	runtime.GOMAXPROCS(4)
	var w sync.WaitGroup
	w.Add(numClients)
	//Test
	var mutex sync.Mutex
	goMap := make(map[string][]byte)
	quitASAP := false
	p := tlfmt.NewProgress("Operating...", iterations*numClients)
	for i := 0; i < numClients; i++ {
		go func(thread int) {
			mutex.Lock()
			//Create client and connect it to the fake server
			c, err := client.Connect(serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			c.SetTimeout = 0
			defer c.Close()
			mutex.Unlock()

			for i := 0; i < iterations; i++ {
				p.Inc()
				op := int(rand.Int31n(int32(2)))
				key := make([]byte, 1)
				key[0] = byte(1)
				value := make([]byte, 4)
				binary.LittleEndian.PutUint32(value, uint32(rand.Int63()))
				runtime.Gosched()
				mutex.Lock()
				if quitASAP {
					mutex.Unlock()
					break
				}
				//fmt.Println(op, key, value)
				switch op {
				case 0:
					goMap[string(key)] = value
					c.Set(key, value)
					mutex.Unlock()
				case 1:
					v2 := goMap[string(key)]
					var v1 []byte
					for i := 1; i < 1000; i = i * 2 {
						if i > 1 {
							time.Sleep(time.Millisecond * time.Duration(i))
						}
						v1, _, _ = c.Get(key)
						if bytes.Equal(v1, v2) {
							break
						}
					}
					if !bytes.Equal(v1, v2) {
						fmt.Println("Mismatch, server returned:", v1,
							"gomap returned:", v2)
						t.Error("Mismatch, server returned:", v1,
							"gomap returned:", v2)
						quitASAP = true
					}
					mutex.Unlock()
					//fmt.Println("GET", key, v1, v2)
				}
			}
			w.Done()
			w.Wait() //CRITICAL: WAIT FOR PENDING WRITE OPERATIONS TO COMPLETE
		}(i)
	}
	w.Wait()
}

func metaTestConsistency(t *testing.T, serverAddr string, numClients, iterations int) {
	runtime.GOMAXPROCS(4)
	var w sync.WaitGroup
	w.Add(numClients)
	//Test
	var mutex sync.Mutex
	goMap := make(map[string][]byte)
	quitASAP := false
	p := tlfmt.NewProgress("Operating...", iterations*numClients)
	for i := 0; i < numClients; i++ {
		go func(thread int) {
			mutex.Lock()
			//Create client and connect it to the fake server
			c, err := client.Connect(serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			mutex.Unlock()

			for i := 0; i < iterations; i++ {
				p.Inc()
				op := int(rand.Int31n(int32(3)))
				key := make([]byte, 1)
				key[0] = byte(1)
				value := make([]byte, 4)
				binary.LittleEndian.PutUint32(value, uint32(rand.Int63()))
				runtime.Gosched()
				mutex.Lock()
				if quitASAP {
					mutex.Unlock()
					break
				}
				switch op {
				case 0:
					///fmt.Println("SET", value)
					goMap[string(key)] = value
					c.Set(key, value)
					mutex.Unlock()
				case 1:
					//fmt.Println("DEL")
					delete(goMap, string(key))
					errs := c.Del(key)
					if errs != nil {
						fmt.Println("Errors:", errs)
					}
					mutex.Unlock()
				case 2:
					//fmt.Println("GET")
					v2 := goMap[string(key)]
					v1, _, _ := c.Get(key)
					if !bytes.Equal(v1, v2) {
						fmt.Println("Mismatch, server returned:", v1,
							"gomap returned:", v2)
						t.Error("Mismatch, server returned:", v1,
							"gomap returned:", v2)
						quitASAP = true
					}
					mutex.Unlock()
				}
			}
			w.Done()
			w.Wait() //CRITICAL: WAIT FOR PENDING WRITE OPERATIONS TO COMPLETE
		}(i)
	}
	w.Wait()
}

//TestClock tests records timestamps synchronization
func TestSingleClock(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	//Client set-up
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	threads := 63
	maxKeySize := 4
	maxValueSize := 4
	numOperations := 1000
	initOps := 1000
	runtime.GOMAXPROCS(runtime.NumCPU())
	timestampMap := make(map[string]time.Time)
	var m sync.Mutex
	var w sync.WaitGroup
	w.Add(threads)
	initTime := time.Now()
	for core := 0; core < threads; core++ {
		go func(core int) {
			rNext := randKVOpGenerator(1, maxKeySize, maxValueSize, core, 64, core)
			for i := -initOps; i < numOperations; i++ {
				_, key, value := rNext()
				t := time.Now()
				if i >= 0 {
					m.Lock()
					timestampMap[string(key)] = t
					m.Unlock()
				}
				c.Set(key, value)
			}
			w.Done()
		}(core)
	}
	w.Wait()
	if testing.Verbose() {
		fmt.Println("Write phase completed in:", time.Now().Sub(initTime))
	}
	time.Sleep(time.Millisecond * 500)
	var maxDiff time.Duration
	var avgDiff time.Duration
	for k, goTime := range timestampMap {
		v, tlTime, _ := c.Get([]byte(k))
		if v == nil {
			t.Error("Get returned nil value")
		}
		diff := tlTime.Sub(goTime)
		avgDiff += diff
		if diff > maxDiff {
			maxDiff = diff
		}
		if diff < 0 {
			t.Error("Warning: negative time difference: ", diff)
		}
	}
	avgDiff = avgDiff / time.Duration(len(timestampMap))
	fmt.Println("Max time difference: ", maxDiff, "\nAverage time difference:", avgDiff)
}

func TestSingleDefrag(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ops := 64
	key := make([]byte, 1)
	value := make([]byte, 1024*1024)
	for i := 0; i < ops; i++ {
		_, err := c.Set(key, value)
		if err != nil {
			t.Fatal(err, i)
		}
		time.Sleep(time.Millisecond * 100)
		c.Del(key)
	}
}
