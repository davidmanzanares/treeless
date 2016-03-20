package tltest

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
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"treeless/src/tlclient"
	"treeless/src/tlhash"
	"treeless/src/tlutils"
)

const testingNumChunks = 8
const benchmarkingNumChunks = 64

var cluster []testServer

func TestMain(m *testing.M) {
	debug.SetTraceback("all")
	cmd := exec.Command("killall", "-s", "INT", "treeless")
	cmd.Run()
	os.Chdir("..")
	cmd = exec.Command("go", "build", "-o", "treeless") //"-race"
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	cmd = exec.Command("cp", "treeless", "testing/") //"-race"
	cmd.Run()
	os.Chdir("testing")
	if err != nil {
		panic("Errors building the program, testing aborted.")
	}
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}
	//CLUSTER INITIALIZATION
	cluster = procStartCluster(5)
	code := m.Run()
	for _, s := range cluster {
		s.kill()
	}
	os.Exit(code)
}

//Test just a few hard-coded operations with one server - one client
func TestSimple(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	waitForServer(addr)
	//Client set-up
	client, err := tlclient.Connect(addr)
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
	value, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	//Del operation
	err = client.Del([]byte("hola"))
	if err != nil {
		t.Fatal(err)
	}

	//Get operation
	value, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get returned string: ", string(value))
	}
}

//TestBigMessages, send 8KB GET, SET messages
func TestBigMessages(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	waitForServer(addr)

	//Client set-up
	client, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	//SET
	_, err = client.Set([]byte("hola"), bytes.Repeat([]byte("X"), 8*1024))
	if err != nil {
		t.Fatal(err)
	}

	//GET
	value, _ := client.Get([]byte("hola"))
	if string(value) != string(bytes.Repeat([]byte("X"), 8*1024)) {
		t.Fatal("Get failed, returned string: ", string(value))
	}
}

//Test just a few hard-coded operations with one server - one client
func TestTimeout(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	waitForServer(addr)
	//Client set-up
	client, err := tlclient.Connect(addr)
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
	value, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	cluster[0].kill()

	time.Sleep(time.Millisecond * 250)

	//Get operation
	tb := time.Now()
	client.GetTimeout = time.Millisecond * 100
	value, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("???")
	}
	log.Println("Timeout:", time.Now().Sub(tb))
}

func TestBasicRebalance(t *testing.T) {
	//Server set-up
	addr1 := cluster[0].create(testingNumChunks, 2)
	waitForServer(addr1)

	//Client set-up
	client, err := tlclient.Connect(addr1)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	//Set operation
	_, err = client.Set([]byte("hola"), []byte("mundo"))
	if err != nil {
		t.Fatal(err)
	}
	//Second server set-up
	cluster[1].assoc(addr1)
	defer cluster[1].kill()
	//Wait for rebalance
	fmt.Println("Server 1 shut down soon...")
	time.Sleep(time.Second * 3)
	//First server shut down
	fmt.Println("Server 1 shut down")
	cluster[0].kill()
	time.Sleep(time.Second)
	//Get operation
	value, _ := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", string(value))
	}

	//Del operation
	client.Del([]byte("hola"))
	//Get operation
	value, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get failed, returned string: ", string(value))
	}
}

//Test lots of operations made by a single client against a single DB server
func TestCmplx1_1(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	waitForServer(addr)
	metaTest(t, addr, 10*1000, 4, 8, 10, 1024)
}

func randKVOpGenerator(maxKeySize, maxValueSize, seed, mult, offset int) func() (op int, k, v []byte) {
	r := rand.New(rand.NewSource(int64(seed)))
	base := make([]byte, 4)
	base2 := make([]byte, 4)
	return func() (op int, k, v []byte) {
		opKeySize := r.Intn(maxKeySize) + 1
		opValueSize := r.Intn(maxValueSize) + 1
		binary.LittleEndian.PutUint32(base, uint32(r.Int31())*uint32(mult)+uint32(offset))
		binary.LittleEndian.PutUint32(base2, uint32(r.Int31())*uint32(mult)+uint32(offset))
		key := bytes.Repeat([]byte(base), opKeySize)[0:opKeySize]
		value := bytes.Repeat([]byte(base2), opValueSize)[0:opValueSize]
		op = 0
		if r.Float32() > 0.5 {
			op = 1
		}
		return op, key, value
	}
}

//This test will make lots of PUT/SET/DELETE operations using a PRNG, then it will use GET operations to check the DB status
func metaTest(t *testing.T, addr string, numOperations, maxKeySize, maxValueSize, threads, maxKeys int) {
	runtime.GOMAXPROCS(threads)
	//Operate on built-in map, DB will be checked against this map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
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
			c, err := tlclient.Connect(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
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
	c, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for key, value := range goMap {
		if len(value) > 128 {
			fmt.Println(123)
		}
		rval, _ := c.Get([]byte(key))
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
		v, _ := c.Get([]byte(key))
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

func TestConsistency(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	waitForServer(addr)
	metaTestConsistency(t, addr, 20, 200)
}

func metaTestConsistency(t *testing.T, serverAddr string, numClients, iterations int) {
	runtime.GOMAXPROCS(4)
	var w sync.WaitGroup
	w.Add(numClients)
	//Test
	var mutex sync.Mutex
	goMap := make(map[string][]byte)
	quitASAP := false
	p := tlutils.NewProgress("Operating...", iterations*numClients)
	for i := 0; i < numClients; i++ {
		go func(thread int) {
			mutex.Lock()
			//Create client and connect it to the fake server
			c, err := tlclient.Connect(serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			c.SetTimeout = 0
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
				//fmt.Println(op, key, value)
				switch op {
				case 0:
					goMap[string(key)] = value
					c.Set(key, value)
					mutex.Unlock()
				case 1:
					//delete(goMap, string(key))
					//c.Del(key)
					mutex.Unlock()
				case 2:
					v2 := goMap[string(key)]
					var v1 []byte
					for i := 1; i < 1000; i = i * 2 {
						time.Sleep(time.Millisecond * time.Duration(i))
						v1, _ = c.Get(key)
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

func TestHotRebalance(t *testing.T) {
	var stop2 func()
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	waitForServer(addr)
	//Client set-up
	c, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	threads := 4
	maxKeySize := 4
	maxValueSize := 4
	numOperations := 50000
	runtime.GOMAXPROCS(runtime.NumCPU())
	//Operate on built-in map, DB will be checked against this map
	goMap := make(map[string][]byte)
	var goDeletes []([]byte)
	for core := 0; core < threads; core++ {
		rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
		for i := 0; i < numOperations; i++ {
			opType, key, value := rNext()
			switch opType {
			case 0:
				//Put
				goMap[string(key)] = value
			case 1:
				//Delete
				//delete(goMap, string(key))
				//goDeletes = append(goDeletes, key)
			}
		}
	}

	//Operate on TreelessDB
	t1 := time.Now()
	var w sync.WaitGroup
	w.Add(threads)
	defer func() {
		if stop2 != nil {
			stop2()
		}
	}()
	p := tlutils.NewProgress("Writting", numOperations*threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
			for i := 0; i < numOperations; i++ {
				//fmt.Println(core, i)
				if core == 0 && i == 0 {
					fmt.Println("Server 2 power up")
					//Second server set-up
					cluster[1].assoc(addr)
					//Wait for rebalance
					time.Sleep(time.Second * 8)
					//First server shut down
					fmt.Println("Server 1 shut down")
					cluster[0].kill()
				}
				p.Inc()
				opType, key, value := rNext()
				switch opType {
				case 0:
					/*if _, ok := goMap[string(key)]; !ok {
						panic(ok)
					}*/
					written, _ := c.Set(key, value)
					for !written { //TODO to sg
						written, _ = c.Set(key, value)
						fmt.Println("SLEEP", core, i)
						time.Sleep(time.Second)

					}
				case 1:
					//c.Del(key)
				}
			}
			w.Done()
		}(core)
	}
	w.Wait()
	if testing.Verbose() {
		fmt.Println("Write phase completed in:", time.Now().Sub(t1))
	}
	p = tlutils.NewProgress("Reading", len(goMap)+len(goDeletes))
	//Check map is in DB
	i := 0
	for key, value := range goMap {
		p.Inc()
		i++
		if len(value) > 128 {
			fmt.Println(123)
		}
		rval, _ := c.Get([]byte(key))
		if !bytes.Equal(rval, value) {
			fmt.Println("GET value differs. Correct value:", value, "Returned value:", rval, "Errors:", err, "ChunkID:", tlhash.FNV1a64([]byte(key))%8)
			t.Fail()
		} else {
			//fmt.Println("OK")
		}
	}

	//Check deleteds aren't in DB
	dels := 0
	for i := 0; i < len(goDeletes); i++ {
		p.Inc()
		key := goDeletes[i]
		_, ok := goMap[string(key)]
		if ok {
			continue
		}
		v, _ := c.Get([]byte(key))
		dels++
		if v != nil {
			t.Fatal("Deleted key present on DB")
		}
	}
	if testing.Verbose() {
		fmt.Println("Present keys tested:", len(goMap))
		fmt.Println("Deleted keys tested:", dels)
	}
	cluster[1].kill()
}

//TestLatency tests latency between a SET operation and a GET operaton that sees the the SET written value
func TestLatency(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	//Client set-up
	c, err := tlclient.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c2, err2 := tlclient.Connect(addr)
	if err2 != nil {
		t.Fatal(err2)
	}
	defer c2.Close()

	type lat struct {
		key string
		t   time.Time
	}

	maxKeySize := 4
	maxValueSize := 4
	numOperations := 20000
	initOps := 10000 //TODO remove
	runtime.GOMAXPROCS(runtime.NumCPU())
	var w sync.WaitGroup
	ch := make(chan lat)
	w.Add(2)
	c.SetTimeout = 0
	var k atomic.Value
	k.Store(1.0)
	go func() {
		rNext := randKVOpGenerator(maxKeySize, maxValueSize, 0, 64, 0)
		for i := -initOps; i < numOperations; i++ {
			_, key, value := rNext()
			t := time.Now()
			c.Set(key, value)
			time.Sleep(time.Duration(k.Load().(float64)) * time.Microsecond)
			if i >= 0 {
				ch <- lat{string(key), t}
				runtime.Gosched()
			}
		}
		close(ch)
		w.Done()
	}()
	oks := 0.
	errors := 0.
	P := 0.999
	lk := 1.0
	go func() {
		for l := range ch {
			v, _ := c2.Get([]byte(l.key))
			if v == nil {
				errors++
				if oks/(oks+errors) < P {
					lk = lk * 1.05
					k.Store(lk)
					oks = 0
					errors = 0.
					//fmt.Println(lk)
				}
			} else {
				oks++
			}
		}
		w.Done()
	}()
	w.Wait()
	fmt.Println("Latency", time.Duration(lk*float64(time.Microsecond)), "at percentile:", oks/(oks+errors)*100.0, "% with", oks+errors, " operations")
}

//TestClock tests records timestamps synchronization
func TestClock(t *testing.T) {
	//time.Sleep(time.Second * 10)
	//panic(111134)
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2)
	defer cluster[0].kill()
	//Client set-up
	c, err := tlclient.Connect(addr)
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
			rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
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
	time.Sleep(time.Second)
	var maxDiff time.Duration
	var avgDiff time.Duration
	for k, goTime := range timestampMap {
		v, tlTime := c.Get([]byte(k))
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

//Test sequential throughtput and consistency
func TestSequential(t *testing.T) {
	addr := cluster[0].create(benchmarkingNumChunks, 2)
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr)
	}
	//time.Sleep(time.Second * 400)
	//Wait for servers
	/*fmt.Println("Waiting for server 192.168.2.100")
	ready := waitForServer("192.168.2.100:9876")
	if !ready {
		t.Fatal("Servers not ready")
	}*/
	/*for i := 1; i < vServers; i++ {
		fmt.Println("Waiting for server 192.168.2." + fmt.Sprint(100+i))
		ready = waitForServer("192.168.2." + fmt.Sprint(100+i) + ":9876")
		if !ready {
			t.Fatal("Servers not ready")
		}
	}*/
	fmt.Println("Servers ready")

	//Initialize vars
	var mutex sync.Mutex
	var w sync.WaitGroup
	goMap := make(map[string][]byte)

	//Sequential workload simulation
	vClients := 10
	operations := 5000
	w.Add(vClients)
	t1 := time.Now()
	p := tlutils.NewProgress("Operating...", operations*vClients)
	for i := 0; i < vClients; i++ {
		go func(thread int) {
			//Create client and connect it to the fake server
			c, err := tlclient.Connect(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			for i := 0; i < operations; i++ {
				//fmt.Println(thread, i)
				p.Inc()
				//Operate
				op := int(rand.Int31n(int32(3)))
				key := make([]byte, 1)
				key[0] = byte(1)
				value := make([]byte, 4)
				binary.LittleEndian.PutUint32(value, uint32(rand.Int63()))
				runtime.Gosched()
				mutex.Lock()
				//fmt.Println(op, key, value)
				switch op {
				case 0:
					goMap[string(key)] = value
					c.Set(key, value)
					mutex.Unlock()
				case 1:
					//delete(goMap, string(key))
					//c.Del(key)
					mutex.Unlock()
				case 2:
					v2 := goMap[string(key)]
					v1, _ := c.Get(key)
					if !bytes.Equal(v1, v2) {
						fmt.Println("Mismatch, server returned:", v1,
							"gomap returned:", v2)
						t.Error("Mismatch, server returned:", v1,
							"gomap returned:", v2)
					}
					mutex.Unlock()
					//fmt.Println("GET", key, v1, v2)
				}
				//Check consistency
				//Ping servers randomly
				//Collect stats

			}
			w.Done()
		}(i)
	}
	w.Wait()
	t2 := time.Now()
	//Print stats
	fmt.Println("\n\nSequential workload simulation - Results")
	fmt.Println("Operations:", operations*vClients, "Throughput:", float64(operations*vClients)/(t2.Sub(t1).Seconds()), "ops/s\n")
}

func TestParallelEach_G90_S10_D0(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2)
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr)
	}
	time.Sleep(time.Second * 5)
	testParallel(addr, t, false, 0.9, 0.1, 0.0)

}
func TestParallelShared_G90_S10_D0(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2)
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr)
	}
	time.Sleep(time.Second * 4)
	testParallel(addr, t, true, 0.9, 0.1, 0.0)

}

func testParallel(addr string, t *testing.T, oneClient bool, pGet, pSet, pDel float64) {
	var w sync.WaitGroup
	vClients := 256
	operations := 1000
	w.Add(vClients)
	//Create client and connect it to the fake server
	var oneC *tlclient.DBClient
	if oneClient {
		var err error
		oneC, err = tlclient.Connect(addr)
		if err != nil {
			t.Fatal(err)
		}
	}
	t1 := time.Now()
	p := tlutils.NewProgress("Operating...", operations*vClients)
	for i := 0; i < vClients; i++ {
		go func(thread int) {
			var c *tlclient.DBClient
			if !oneClient {
				var err error
				c, err = tlclient.Connect(addr)
				if err != nil {
					log.Println(err)
					w.Done()
					return
				}
				defer c.Close()
			} else {
				c = oneC
			}
			for i := 0; i < operations; i++ {
				p.Inc()
				//Operate
				op := rand.Float64()
				key := make([]byte, 1)
				key[0] = byte(1)
				value := make([]byte, 4)
				binary.LittleEndian.PutUint32(value, uint32(rand.Int63()))
				//fmt.Println(op, key, value)
				switch {
				case op < pGet:
					c.Get(key)
				case op < pGet+pSet:
					c.Set(key, value)
				default:
					//delete(goMap, string(key))
					//c.Del(key)
				}
				//Ping servers randomly
				//Collect stats
			}
			w.Done()
		}(i)
	}
	w.Wait()
	t2 := time.Now()
	//Print stats
	str := "1 client"
	if !oneClient {
		str = "N clients"
	}
	str += " " + fmt.Sprint(vClients) + " parralel operators"
	fmt.Println("\nMassive parallel workload simulation "+str, "Get/Set/Del", pGet, pSet, pDel, "- Results")
	fmt.Println("Operations:", operations*vClients, "Throughput:", float64(operations*vClients)/(t2.Sub(t1).Seconds()), "ops/s\n")
}

//Benchmark GET operations by issuing lots of GET operations from different goroutines.
//The DB is clean, all operations will return a "Key not present" error
func BenchmarkGetUnpopulated1Server(b *testing.B) {
	fmt.Println(b.N)
	//Server set-up
	addr := cluster[0].create(benchmarkingNumChunks, 2)
	defer cluster[0].kill()
	for i := 1; i < len(cluster); i++ {
		//cluster[i].assoc(addr)
		//defer cluster[i].kill()
	}
	time.Sleep(time.Second * 5)

	//Clients set-up
	var clients [8]*tlclient.DBClient
	for i := 0; i < 8; i++ {
		c, err := tlclient.Connect(addr)
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		clients[i] = c
	}
	maxKeySize := 4
	maxValueSize := 4
	gid := uint64(0)
	b.ResetTimer()
	b.SetParallelism(256)
	b.RunParallel(
		func(pb *testing.PB) {
			core := int(atomic.AddUint64(&gid, 1))
			c := clients[1]
			rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
			for pb.Next() {
				_, key, _ := rNext()
				c.Get(key)
			}

		})
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
