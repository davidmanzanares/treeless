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
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"treeless/src/com"
	"treeless/src/hash"
	"treeless/src/sg"
	"treeless/src/tlutils"
)

func TestMain(m *testing.M) {
	cmd := exec.Command("killall", "-s", "INT", "treeless")
	cmd.Run()
	os.Chdir("..")
	cmd = exec.Command("go", "build", "-o", "treeless") //"-race"
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		panic("Errors building the program, testing aborted.")
	}
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}
	os.Exit(m.Run())
}

var id = 0

const useProcess = false
const numChunks = 8

func LaunchServer(assoc string) (addr string, stop func()) {
	dbpath := "testDB" + fmt.Sprint(id)
	var cmd *exec.Cmd
	var s *tlsg.DBServer
	if assoc == "" {
		id = 0
		dbpath = "testDB" + fmt.Sprint(id)
		if useProcess {
			cmd = exec.Command("./treeless", "-create", "-port", fmt.Sprint(10000+id), "-dbpath", dbpath) //, "-cpuprofile"
		} else {
			s = tlsg.Start("", 10000+id, numChunks, 2, dbpath)
		}
	} else {
		if useProcess {
			cmd = exec.Command("./treeless", "-assoc", assoc, "-port", fmt.Sprint(10000+id), "-dbpath", dbpath)
		} else {
			s = tlsg.Start(assoc, 10000+id, numChunks, 2, dbpath)
		}
	}
	if useProcess && testing.Verbose() {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	id++
	if useProcess {
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
	}
	newAddr := string(tlcom.GetLocalIP()) + ":" + fmt.Sprint(10000+id-1)
	for i := 0; i < 50; i++ {
		time.Sleep(time.Millisecond * 50)
		client, err := tlsg.Connect(newAddr)
		if err == nil {
			client.Close()
			break
		}
	}
	return newAddr,
		func() {
			if useProcess {
				cmd.Process.Signal(os.Interrupt)
			} else {
				s.Stop()
			}
			time.Sleep(time.Millisecond * 10)
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
	client, err := tlsg.Connect(addr)
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
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	client, err := tlsg.Connect(addr)
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

func TestBasicRebalance(t *testing.T) {
	//Server set-up
	addr1, stop1 := LaunchServer("")
	//Client set-up
	client, err := tlsg.Connect(addr1)
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
	_, stop2 := LaunchServer(addr1)
	defer stop2()
	//Wait for rebalance
	time.Sleep(time.Second * 5)
	//First server shut down
	fmt.Println("Server 1 shut down")
	stop1()
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
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	client, err := tlsg.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	metaTest(t, client, 10*1000, 4, 8, 10, 1024)
}

//Test lots of operations made by multiple clients against a single DB server
func TestCmplxN_1(t *testing.T) {
	//metaTest(10*1000, 10, 40, 10)
}

//Test lots of operations made by multiple clients against multiple DB servers
func TestCmplxN_N(t *testing.T) {
	//metaTest(10*1000, 10, 40, 10)
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
func metaTest(t *testing.T, c *tlsg.DBClient, numOperations, maxKeySize, maxValueSize, threads, maxKeys int) {
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

func TestHotRebalance(t *testing.T) {
	var stop2 func()
	//Server set-up
	addr, stop := LaunchServer("")
	//Client set-up
	c, err := tlsg.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	threads := 4
	maxKeySize := 4
	maxValueSize := 4
	numOperations := 20000
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
				delete(goMap, string(key))
				goDeletes = append(goDeletes, key)
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
	p := tlutils.NewProgress("Writting", numOperations*2-1)
	progress := uint64(0)
	for core := 0; core < threads; core++ {
		go func(core int) {
			rNext := randKVOpGenerator(maxKeySize, maxValueSize, core, 64, core)
			for i := 0; i < numOperations; i++ {
				//fmt.Println(core, i)
				if core == 0 && i == 0 {
					fmt.Println("Server 2 power up")
					//Second server set-up
					_, stop2 = LaunchServer(addr)
					//Wait for rebalance
					time.Sleep(time.Second * 10)
					//First server shut down
					fmt.Println("Server 1 shut down")
					stop()
				} else if core == 1 {
					atomic.AddUint64(&progress, 1)
					p.Set(int(atomic.LoadUint64(&progress)))
				} else if core == 0 {
					atomic.AddUint64(&progress, 1)
					p.Set(int(atomic.LoadUint64(&progress)))
				}
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
	p = tlutils.NewProgress("Reading", len(goMap)+len(goDeletes))
	//Check map is in DB
	i := 0
	for key, value := range goMap {
		p.Set(i)
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
		p.Set(i + len(goMap))
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

//TestLatency tests latency between a SET operation and a GET operaton that sees the the SET written value
func TestLatency(t *testing.T) {
	//Server set-up
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	c, err := tlsg.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c2, err2 := tlsg.Connect(addr)
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
	numOperations := 40000
	initOps := 40000
	runtime.GOMAXPROCS(runtime.NumCPU())
	var w sync.WaitGroup
	ch := make(chan lat)
	w.Add(2)
	k := float64(10)
	go func() {
		rNext := randKVOpGenerator(maxKeySize, maxValueSize, 0, 64, 0)
		for i := -initOps; i < numOperations; i++ {
			_, key, value := rNext()
			t := time.Now()
			c.Set(key, value)
			time.Sleep(time.Duration(k) * time.Microsecond)
			if i >= 0 {
				ch <- lat{string(key), t}
			}
		}
		close(ch)
		w.Done()
	}()
	oks := 0
	go func() {
		for l := range ch {
			v, _ := c2.Get([]byte(l.key))
			if v == nil {
				k = k * 1.05
				oks = 0
			} else {
				oks++
			}
		}
		w.Done()
	}()
	w.Wait()
	fmt.Println("Latency", time.Duration(k)*time.Microsecond, "Error:", 1.0/float64(oks)*100.0, "%")
}

//TestClock tests records timestamps synchronization
func TestClock(t *testing.T) {
	//Server set-up
	addr, stop := LaunchServer("")
	defer stop()
	//Client set-up
	c, err := tlsg.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer time.Sleep(time.Second)
	defer c.Close()

	threads := 63
	maxKeySize := 4
	maxValueSize := 4
	numOperations := 1000
	initOps := 40000
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
		_, tlTime := c.Get([]byte(k))
		if err != nil {
			panic(err)
		}
		diff := tlTime.Sub(goTime)
		avgDiff += diff
		if diff > maxDiff {
			maxDiff = diff
		}
		if diff < 0 {
			fmt.Println("Warning: negative time difference: ", diff)
		}
	}
	avgDiff = avgDiff / time.Duration(len(timestampMap))
	fmt.Println("Max time difference: ", maxDiff, "\nAverage time difference:", avgDiff)
}

//Benchmark GET operations by issuing lots of GET operations from different goroutines.
//The DB is clean, all operations will return a "Key not present" error
func BenchmarkGetUnpopulated(b *testing.B) {
	fmt.Println(b.N)
	//Server set-up
	addr, stop := LaunchServer("")
	defer stop()
	//Clients set-up
	var clients [8]*tlsg.DBClient
	for i := 0; i < 8; i++ {
		c, err := tlsg.Connect(addr)
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
			c := clients[core%5]
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
