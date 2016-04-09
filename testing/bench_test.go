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
)

func TestBenchParallelEachS1_G90_S10_D0(t *testing.T) {
	testBenchParallel(t, false, 0.9, 0.1, 0.0, 1, 100000)

}
func TestBenchParallelSharedS1_G90_S10_D0(t *testing.T) {
	testBenchParallel(t, true, 0.9, 0.1, 0.0, 1, 1000000)
}

func testBenchParallel(t *testing.T, oneClient bool, pGet, pSet, pDel float32, servers int, operations int) {
	vClients := 1024
	addr := cluster[0].create(benchmarkingNumChunks, 2, false)
	for i := 1; i < servers; i++ {
		cluster[i].assoc(addr, ultraverbose)
	}
	var w sync.WaitGroup
	w.Add(vClients)
	runtime.GOMAXPROCS(runtime.NumCPU())
	clients := make([]*client.DBClient, vClients)
	if oneClient {
		c, err := client.Connect(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		for i := range clients {
			clients[i] = c
		}
	} else {
		for i := range clients {
			c, err := client.Connect(addr)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			clients[i] = c
		}
	}
	//p := tlfmt.NewProgress("Operating...", operations)
	ops := int32(0)
	runtime.GC()
	runtime.Gosched()
	t1 := time.Now()
	for i := 0; i < vClients; i++ {
		go func(thread int) {
			c := clients[thread]
			value := make([]byte, 4)
			rNext := randKVOpGenerator(4, 4, 4, thread+1, 2048, thread+1)
			for atomic.AddInt32(&ops, 1) <= int32(operations) {
				//p.Inc()
				//Operate
				op := rand.Float32()
				_, key, _ := rNext()
				//fmt.Println(op, key, value)
				switch {
				case op < pGet:
					c.Get(key)
				case op < pGet+pSet:
					binary.LittleEndian.PutUint32(value, uint32(rand.Int()))
					c.Set(key, value)
				default:
					c.Del(key)
				}
			}
			w.Done()
		}(i)
	}
	w.Wait()
	t2 := time.Now()
	//Print stats
	fmt.Println("Throughput:", float64(operations)/(t2.Sub(t1).Seconds()), "ops/s")
}

func TestBenchSequentialS1(t *testing.T) {
	testBenchSequential(t, 1)
}

//Test sequential throughtput and consistency
func testBenchSequential(t *testing.T, servers int) {
	addr := cluster[0].create(benchmarkingNumChunks, 2, false)
	for i := 1; i < servers; i++ {
		cluster[i].assoc(addr, false)
	}

	//Initialize vars
	goMap := make(map[string][]byte)

	//Sequential workload simulation
	operations := 50000
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t1 := time.Now()
	for i := 0; i < operations; i++ {
		//Operate
		op := int(rand.Int31n(int32(3)))
		key := make([]byte, 1)
		key[0] = byte(1)
		value := make([]byte, 4)
		binary.LittleEndian.PutUint32(value, uint32(rand.Int63()))
		//fmt.Println(op, key, value)
		switch op {
		case 0:
			goMap[string(key)] = value
			c.Set(key, value)
		case 1:
			delete(goMap, string(key))
			c.Del(key)
		case 2:
			v2 := goMap[string(key)]
			v1, _ := c.Get(key)
			if !bytes.Equal(v1, v2) {
				fmt.Println("Mismatch, server returned:", v1,
					"gomap returned:", v2)
				t.Error("Mismatch, server returned:", v1,
					"gomap returned:", v2)
			}
		}
	}
	t2 := time.Now()
	//Print stats
	fmt.Println("Throughput:", float64(operations)/(t2.Sub(t1).Seconds()), "ops/s")
}
