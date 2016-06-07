package test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"treeless/client"
	"treeless/hashing"
	"treeless/tlfmt"
)

func TestMultiBasicRebalance(t *testing.T) {
	//Server set-up
	addr1 := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	waitForServer(addr1)

	//Client set-up
	client, err := client.Connect(addr1)
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
	cluster[1].assoc(addr1, ultraverbose, false)
	defer cluster[1].kill()
	//Wait for rebalance
	fmt.Println("Server 1 shut down soon...")
	time.Sleep(time.Second * 6)
	//First server shut down
	fmt.Println("Server 1 shut down")
	cluster[0].kill()
	time.Sleep(time.Millisecond * 100)
	//Get operation
	value, ts, g := client.Get([]byte("hola"))
	if string(value) != "mundo" {
		t.Fatal("Get failed, returned string: ", (value), ts, g)
	}

	//Del operation
	client.Del([]byte("hola"))
	//Get operation
	value, _, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get failed 2, returned string: ", string(value))
	}
}

func TestMultiHotRebalance(t *testing.T) {
	var stop2 func()
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	waitForServer(addr)
	//Client set-up
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	threads := 4
	maxKeySize := 4
	maxValueSize := 4
	numOperations := 120000
	runtime.GOMAXPROCS(runtime.NumCPU())
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
	defer func() {
		if stop2 != nil {
			stop2()
		}
	}()
	p := tlfmt.NewProgress("Writting", numOperations*threads)
	for core := 0; core < threads; core++ {
		go func(core int) {
			rNext := randKVOpGenerator(1, maxKeySize, maxValueSize, core, 64, core)
			for i := 0; i < numOperations; i++ {
				//fmt.Println(core, i)
				if core == 0 && i == 0 {
					time.Sleep(time.Second * 1)
					fmt.Println("Server 2 power up")
					//Second server set-up
					cluster[1].assoc(addr, ultraverbose, false)
					//Wait for rebalance
					time.Sleep(time.Second * 5)
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
						time.Sleep(time.Millisecond * 300)
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
	p = tlfmt.NewProgress("Reading", len(goMap)+len(goDeletes))
	//Check map is in DB
	i := 0
	for key, value := range goMap {
		p.Inc()
		i++
		if len(value) > 128 {
			fmt.Println(123)
		}
		rval, _, _ := c.Get([]byte(key))
		if !bytes.Equal(rval, value) {
			fmt.Println("GET value differs. Correct value:", value, "Returned value:", rval, "Errors:", err, "ChunkID:", hashing.FNV1a64([]byte(key))%8)
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
	cluster[1].kill()
}

func TestMultiNodeRevival(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	//Write
	c.Set([]byte("hello"), []byte("world"))

	addr2 := cluster[1].assoc(addr, ultraverbose, false)
	time.Sleep(time.Second * 6)

	fmt.Println("Server 0 down")
	cluster[0].kill()

	cluster[0].assoc(addr2, ultraverbose, false)
	time.Sleep(time.Second * 9)

	fmt.Println("Server 1 down")
	cluster[1].kill()

	//Read
	time.Sleep(time.Millisecond * 100)
	v, _, _ := c.Get([]byte("hello"))
	if string(v) != "world" {
		t.Fatal("Mismatch:", v)
	}
}

func TestMultiConsistency(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr, ultraverbose, false)
		defer cluster[i].kill()
	}
	waitForServer(addr)
	metaTestConsistency(t, addr, 20, 200)
}

func TestMultiConsistencyAsyncSet(t *testing.T) {
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr, ultraverbose, false)
		defer cluster[i].kill()
	}
	waitForServer(addr)
	metaTestConsistencyAsyncSet(t, addr, 20, 200)
}

func TestMultiCAS(t *testing.T) {
	runtime.GOMAXPROCS(5)
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	defer cluster[0].kill()
	for i := 1; i < len(cluster); i++ {
		cluster[i].assoc(addr, ultraverbose, false)
		defer cluster[i].kill()
	}
	time.Sleep(time.Second * 8)
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err, "Should increase sleep time?")
	}
	defer c.Close()

	//Func Inc
	tries := uint64(0)
	inc := func(c *client.DBClient, key []byte) {
		written := false
		for !written {
			oldv, t, _ := c.Get(key)
			//fmt.Println(oldv, t)
			x := binary.LittleEndian.Uint32(oldv)
			//fmt.Println(x)
			x++
			value := make([]byte, 8)
			binary.LittleEndian.PutUint32(value, uint32(x))
			binary.LittleEndian.PutUint32(value[4:8], uint32(rand.Int63()))
			written, _ = c.CAS(key, value, t, oldv)
			atomic.AddUint64(&tries, 1)
			if !written {
				time.Sleep(time.Millisecond)
			}
		}
	}
	key := make([]byte, 1)
	key[0] = byte(1)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, uint32(0))
	written, errs := c.CAS(key, value, time.Unix(0, 0), nil)
	if !written {
		t.Fatal("Initial CAS failed", errs)
	}
	v, _, _ := c.Get(key)
	//fmt.Println(oldv, t)
	n := binary.LittleEndian.Uint32(v)
	if n != 0 {
		t.Fatal("Initial CAS failed", n, v)
	}
	ops := 100
	clients := 100
	var w sync.WaitGroup
	w.Add(clients)
	for c := 0; c < clients; c++ {
		go func() {
			c, err := client.Connect(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			for i := 0; i < ops; i++ {
				inc(c, key)
			}
			w.Done()
			time.Sleep(time.Second)
		}()
	}
	w.Wait()
	//fmt.Println("SLEEEP")
	//time.Sleep(time.Minute)
	//Get & check
	v, _, _ = c.Get(key)
	x := int(binary.LittleEndian.Uint32(v))
	fmt.Println("Tries:", tries, "Total operations:", ops*clients, "Clients:", clients)
	if x != ops*clients {
		t.Fatal("Mismatch:", ops*clients, "!=", x, "Value:", v)
	}
}

func TestMultiReadRepair(t *testing.T) {
	if !cluster[0].testCapability(capDisconnect) {
		t.Skip("Cluster doesn't support disconnections")
	}
	//Start A
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	//Start B
	cluster[1].assoc(addr, ultraverbose, false)
	time.Sleep(time.Second * 8)

	fmt.Println("Disconnect A")
	cluster[0].disconnect()
	time.Sleep(time.Second * 8)

	//SET
	c.Set([]byte("hola"), []byte("mundo"))

	//Connect A (A should have the old value)
	fmt.Println("Reconnect A")
	cluster[0].reconnect()
	time.Sleep(time.Second * 3)

	//GET (should trigger the read-repairing system)
	c.Get([]byte("hola"))

	//Kill B (A should have the new value)
	cluster[1].kill()
	time.Sleep(time.Second)

	//GET and check
	v, _, _ := c.Get([]byte("hola"))
	if string(v) != "mundo" {
		t.Fatal("Mismatch", string(v))
	}
}

func TestMultiBackwardsRepair(t *testing.T) {
	if !cluster[0].testCapability(capDisconnect) {
		t.Skip("Cluster doesn't support disconnections")
	}
	//Start A
	addr := cluster[0].create(testingNumChunks, 2, ultraverbose, false)
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	//Start B
	cluster[1].assoc(addr, ultraverbose, false)
	time.Sleep(time.Second * 8)

	fmt.Println("Disconnect A")
	cluster[0].disconnect()
	time.Sleep(time.Second * 8)

	//SET
	c.Set([]byte("hola"), []byte("mundo"))

	//Connect A (A should have the old value)
	fmt.Println("Reconnect A")
	cluster[0].reconnect()
	time.Sleep(time.Second * 15)

	//Kill B (A should have the new value)
	cluster[1].kill()
	time.Sleep(time.Second * 5)

	//GET and check
	v, _, _ := c.Get([]byte("hola"))
	if string(v) != "mundo" {
		t.Fatal("Mismatch", string(v))
	}
}

//Test just a few hard-coded operations to test the persistence
func TestMultiOpen(t *testing.T) {
	//Server set-up
	addr := cluster[0].create(testingNumChunks, 1, ultraverbose, false)
	cluster[1].assoc(addr, ultraverbose, false)
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
	cluster[1].close()
	addr = cluster[0].create(testingNumChunks, 1, ultraverbose, true)
	cluster[1].assoc(addr, ultraverbose, true)

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
	cluster[1].close()
	addr = cluster[0].create(testingNumChunks, 1, ultraverbose, true)
	cluster[1].assoc(addr, ultraverbose, true)

	//Get operation
	value, _, _ = client.Get([]byte("hola"))
	if value != nil {
		t.Fatal("Get 2 returned string: ", string(value))
	}

	cluster[0].close()
	cluster[1].close()
}
