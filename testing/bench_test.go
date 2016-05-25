package test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"treeless/client"
	"treeless/tlfmt"
)

func TestBenchParallelEachS1_Get(t *testing.T) {
	testBenchParallel(t, 256, 256, 1.0, 0., 0.0, 0, 0, 0, 1, 1000000, 0, "", 1000000)
}

func TestBenchParallelSharedS1_Get(t *testing.T) {
	testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, 10000000, 0, "", 1000000)
}
func TestBenchParallelSharedS1_GetFilled(t *testing.T) {
	testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, 10000000, 1000000, "", 1000000)
}

func TestBenchParallelSharedS1_GetClients(t *testing.T) {
	for i := 100; i < 4000; i += 200 {
		testBenchParallel(t, i, 1, 1.0, 0.0, 0.0, 0, 0, 0, 1, 1000000, 0, "", 1000000)
	}
}

func TestBenchParallelSharedS1_MixGet60Del10AsyncSet30(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.6, 0.0, 0.1, 0, 0.3, 0, 1, 1000000, 1000000, "", 1000000)
}

func TestBenchParallelSharedS1_Set(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 1.0, 0.0, 0, 0, 0, 1, 10000000, 0, "", 1000000)
}
func TestBenchParallelSharedS1_SetFilled(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 1.0, 0.0, 0, 0, 0, 1, 10000000, 1000000, "", 1000000)
}
func TestBenchParallelSharedS1_Del(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 1.0, 0, 0, 0, 1, 10000000, 0, "", 1000000)
}
func TestBenchParallelSharedS1_DelFilled(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 1.0, 0, 0, 0, 1, 10000000, 1000000, "", 1000000)
}
func TestBenchParallelSharedS1_CAS(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 0., 1, 0, 0, 1, 10000000, 0, "", 1000000)
}
func TestBenchParallelSharedS1_CASFilled(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 0., 1, 0, 0, 1, 10000000, 1000000, "", 1000000)
}
func TestBenchParallelSharedS1_AsyncSet(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 0.0, 0, 1, 0, 1, 10000000, 0, "", 1000000)
}
func TestBenchParallelSharedS1_AsyncSetFilled(t *testing.T) {
	testBenchParallel(t, 10000, 10, 0.0, 0.0, 0.0, 0, 1, 0, 1, 10000000, 1000000, "", 1000000)
}

func TestBenchParallelSharedSX_GetFilled(t *testing.T) {
	for i := 1; i <= len(cluster); i++ {
		testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, i, 10000000, 0, "", 1000000)
	}
}

func TestBenchParallelSharedS1_GetFilledDisk(t *testing.T) {
	for i := 100; i < 80000; i += 4000 {
		testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, 500000, i*1000, "", 1000000)
	}
}

func TestBenchParallelSharedS1_GetFilledDiskParallelism(t *testing.T) {
	addr := cluster[0].create(32, 1, ultraverbose)
	testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, 1, 50*1000*1000, addr, 50*1000*1000)
	for i := 10; i < 4100; i *= 2 {
		fmt.Println("\n\n\n")
		testBenchParallel(t, i, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, 400*1000, 0, addr, 50*1000*1000)
	}
	cluster[0].kill()
}

func TestBenchParallelSharedS1_SetFilledDisk(t *testing.T) {
	for i := 1; i < 180000; i += 4000 {
		addr := cluster[0].create(32, 1, ultraverbose)
		ops := 10000000
		if i > 16000 {
			ops = 800000
		}
		if i > 20000 {
			ops = 200000
		}
		testBenchParallel(t, 10000, 10, 1.0, 0.0, 0.0, 0, 0, 0, 1, ops, i*1000, addr, i*1000)
		testBenchParallel(t, 10000, 10, 0.0, 1.0, 0.0, 0, 0, 0, 1, ops, 0, addr, i*1000)
		testBenchParallel(t, 10000, 10, 0.0, 0.0, 0.0, 0, 0, 1, 1, 4000000, 0, addr, i*1000)
		cluster[0].kill()
		fmt.Println("\n\n\n")
		if i == 1 {
			i = 0
		}
		//testBenchParallel(t, 1000, 10, 0.5, 0.5, 0.0, 0, 0, 1, 500000, 0)
	}
}

func testBenchParallel(t *testing.T, threads int, numClients int, pGet, pSet, pDel, pCAS, pAsyncSet, pSetNew float32,
	servers int, operations int, fill int, addr string, space int) {
	if servers > len(cluster) {
		t.Skip("Cluster is too small")
	}
	if addr == "" {
		addr = cluster[0].create(32, 1, ultraverbose)
		for i := 1; i < servers; i++ {
			cluster[i].assoc(addr, ultraverbose)
		}
	}
	//addr := "169.254.9.58:9876"
	/*addr := "127.0.0.1:9876"
	fmt.Println("START THE SERVER")
	time.Sleep(time.Second * 5)
	fmt.Println("SERVER STARTED?")*/
	//addr := "127.0.0.1:10000"

	fmt.Println("NumClients:", numClients, "Numthreads", threads, "Servers", servers, "Preconditioning", fill, "Operations", operations)
	fmt.Printf("pGet=%v pSet=%v pDel=%v pCAS=%v pAsyncSet=%v pSetNew=%v\n", pGet, pSet, pDel, pCAS, pAsyncSet, pSetNew)

	var w, wpre sync.WaitGroup
	w.Add(threads)
	runtime.GOMAXPROCS(runtime.NumCPU())
	clients := make([]*client.DBClient, numClients)
	for i := range clients {
		c, err := client.Connect(addr)
		c.SetBufferingMode(client.Buffered)
		if err != nil {
			t.Fatal(i, err)
		}
		c.SetTimeout = time.Second * 4
		c.GetTimeout = time.Second * 4
		defer c.Close()
		clients[i] = c
	}
	if servers > 1 {
		//Wait for rebalance
		time.Sleep(time.Second * 35)
	}
	if fill > 0 {
		wpre.Add(1000)
		pp := tlfmt.NewProgress("Preconditioning...", fill)

		for i := 0; fill > 0 && i < fill; i += fill / 1000 {
			go func(i int) {
				key := make([]byte, 4)
				value := make([]byte, 220)
				c := clients[0]
				for j := i; j < i+fill/1000; j++ {
					pp.Inc()
					binary.LittleEndian.PutUint32(key, uint32(j))
					wr, err := c.Set(key, value)
					if !wr || err != nil {
						fmt.Println(wr, err)
					}
				}
				wpre.Done()
			}(i)
		}
		wpre.Wait()
	}
	//time.Sleep(time.Second * 5)
	p := tlfmt.NewProgress("Operating...", operations)
	latencies := make([]float64, operations)
	ops := int32(0)
	runtime.Gosched()
	runtime.GC()
	t1 := time.Now()
	for i := 0; i < threads; i++ {
		go func(thread int) {
			c := clients[thread%numClients]
			key := make([]byte, 4)
			value := make([]byte, 4)
			opID := atomic.AddInt32(&ops, 1)
			for opID < int32(operations) {
				binary.LittleEndian.PutUint32(key, uint32(rand.Int31n(int32(space))))
				p.Inc()
				//Operate
				op := rand.Float32()
				//fmt.Println(op, key, value)
				t1 := time.Now()
				switch {
				case op < pGet:
					for i := 0; ; i++ {
						if i == 5 {
							t.Error("5 timeouts")
						}
						_, _, r := c.Get(key)
						if r {
							break
						}
						time.Sleep(time.Millisecond * 500)
					}
				case op < pGet+pSet:
					wr, err := c.Set(key, value)
					if !wr || err != nil {
						fmt.Println(wr, err)
					}
				case op < pGet+pSet+pDel:
					c.Del(key)
				case op < pGet+pSet+pDel+pCAS:
					c.CAS(key, value, time.Time{}, value)
				case op < pGet+pSet+pDel+pCAS+pAsyncSet:
					c.SetAsync(key, value)
				default:
					binary.LittleEndian.PutUint32(key, uint32(int32(80000*1000)+rand.Int31n(10000*1000)))
					wr, err := c.Set(key, value)
					if !wr || err != nil {
						fmt.Println(wr, err)
					}
				}
				latencies[opID] = time.Now().Sub(t1).Seconds()
				opID = atomic.AddInt32(&ops, 1)
			}
			if pCAS > 0 {
				c.Set(key, value)
			}
			w.Done()
		}(i)
	}
	w.Wait()
	t2 := time.Now()
	//Print stats
	fmt.Println("Throughput:", float64(operations)/(t2.Sub(t1).Seconds()), "ops/s", "Execution time:", t2.Sub(t1).Seconds(), "s")
	sort.Float64Slice(latencies).Sort()
	fmt.Println("50%", latencies[operations*50/100]*1000.0, "ms", "\t99%", latencies[operations*99/100]*1000.0, "ms",
		"\n99.9%", latencies[operations*999/1000]*1000.0, "ms", "\t100%", latencies[operations-1]*1000.0, "ms\n")

}

func TestBenchSequentialS1(t *testing.T) {
	testBenchSequential(t, 1)
}

//Test sequential throughtput and consistency
func testBenchSequential(t *testing.T, servers int) {
	addr := cluster[0].create(testingNumChunks, 2, false)
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
			v1, _, _ := c.Get(key)
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
