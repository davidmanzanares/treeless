package test

import (
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

//scalability

type benchCluster struct {
	precondition int
	servers      int
	addr         string
}

type benchOpMix struct {
	pGet, pSet, pDel, pCAS, pAsyncSet, pSetNew float32
}

type benchDef struct {
	threads, clients  int
	mix               benchOpMix
	operations, space int
	bufferingMode     client.BufferingMode
}

func TestBenchOpGet(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pGet: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchOpSet(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pSet: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchOpDel(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pDel: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchOpCAS(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pCAS: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchOpSetNew(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pSetNew: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchOpAsyncSet(t *testing.T) {
	c := testBenchPrepareCluster(t, 3000*1000, 4, 1)
	bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
	bdef.mix = benchOpMix{pAsyncSet: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchSequential(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	bdef := benchDef{threads: 1, clients: 1, operations: 150 * 1000, space: 1000 * 1000, bufferingMode: client.NoDelay}
	bdef.mix = benchOpMix{pGet: 1}
	testBenchParallel(t, c, bdef)
}

func TestBenchClientParallelism(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	for i := 1; i < 2000; i *= 2 {
		fmt.Println(i, "clients")
		bdef := benchDef{threads: 4000, clients: i, operations: 2500 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func TestBenchThreadParallelism(t *testing.T) {
	c := testBenchPrepareCluster(t, 1000*1000, 4, 1)
	for i := 64; i < 30000; i *= 2 {
		fmt.Println(i, "threads")
		bdef := benchDef{threads: i, clients: 10, operations: 3000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func TestBenchDiskParallelism(t *testing.T) {
	c := testBenchPrepareCluster(t, 50*1000*1000, 220, 1)
	for i := 1; i < 1000; i *= 2 {
		fmt.Println(i, "clients")
		bdef := benchDef{threads: 2000, clients: i, operations: 150 * 1000, space: 50 * 1000 * 1000, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func TestBenchDiskVsRAM(t *testing.T) {
	for i := 1; i < 120; i += 10 {
		fmt.Println(float64(i*(220+4+8))/1024.0, "GB")
		c := testBenchPrepareCluster(t, i*1024*1024, 220, 1)
		bdef := benchDef{threads: 2000, clients: 128, operations: 350 * 1000, space: i * 1024 * 1024, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func TestBenchValueSize(t *testing.T) {
	for i := 4; i < 4*1024; i *= 2 {
		fmt.Println(i, "bytes")
		c := testBenchPrepareCluster(t, 1000*1000, i, 1)
		bdef := benchDef{threads: 4000, clients: 10, operations: 5 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func TestBenchScalability(t *testing.T) {
	if len(cluster) < 2 {
		t.Skip("Cluster is too small")
	}
	for i := 1; i <= len(cluster); i++ {
		fmt.Println(i, "servers")
		c := testBenchPrepareCluster(t, 1000*1000, 4, i)
		bdef := benchDef{threads: 4000, clients: 4, operations: 2 * 1000 * 1000, space: 1000 * 1000, bufferingMode: client.Buffered}
		bdef.mix = benchOpMix{pGet: 1}
		testBenchParallel(t, c, bdef)
	}
}

func testBenchPrepareCluster(t *testing.T, precondition, preconditionValueSize, servers int) benchCluster {
	if servers > len(cluster) {
		t.Skip("Cluster is too small")
	}
	bc := benchCluster{precondition: precondition, servers: servers}
	bc.addr = cluster[0].create(16, 1, ultraverbose, false)
	for i := 1; i < servers; i++ {
		cluster[i].assoc(bc.addr, ultraverbose, false)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	if servers > 1 {
		//Wait for rebalance
		time.Sleep(time.Second * 35)
	}
	//Preconditioning
	if precondition < 1 {
		return bc
	}
	c, err := client.Connect(bc.addr)
	c.SetTimeout = time.Second * 5
	c.SetBufferingMode(client.Buffered)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var w sync.WaitGroup
	threads := 1000
	w.Add(threads)
	t1 := time.Now()
	p := tlfmt.NewProgress(fmt.Sprint("Preconditioning ", precondition, " x ", preconditionValueSize, "bytes"), precondition)
	index := uint32(0)
	for i := 0; i < threads; i++ {
		go func(i int) {
			key := make([]byte, 4)
			value := make([]byte, preconditionValueSize)
			for {
				p.Inc()
				k := atomic.AddUint32(&index, 1)
				if k >= uint32(precondition) {
					break
				}
				binary.LittleEndian.PutUint32(key, k)
				if true {
					wr, err := c.Set(key, value)
					if !wr || err != nil {
						fmt.Println("Error at preconditioning", wr, err)
					}
				} else {
					c.AsyncSet(key, value)
				}
			}
			w.Done()
		}(i)
	}
	w.Wait()
	fmt.Println("Precondition time", time.Now().Sub(t1))
	return bc
}

/*
func TestBenchParallelEachS1_Get(t *testing.T) {
	testBenchParallel(t, 10000, 256, 1.0, 0., 0.0, 0, 0, 0, 1, 1000000, 0, "", 1000000)
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

//fio --name=randread --ioengine=sync --rw=randread --bs=4k --direct=1 --size=1G --numjobs=32 --runtime=240 --group_reporting

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
	addr := cluster[0].create(256, 1, ultraverbose)
	testBenchParallel(t, 10000, 32, 1.0, 0.0, 0.0, 0, 0, 0, 1, 1, 50*1000*1000, addr, 50*1000*1000)
	for i := 2; i < 500; i *= 2 {
		fmt.Println("\n\n\n")
		testBenchParallel(t, 1000, i, 1.0, 0.0, 0.0, 0, 0, 0, 1, 400*1000, 0, addr, 50*1000*1000)
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
}*/

func testBenchParallel(t *testing.T, cluster benchCluster, bdef benchDef) {
	mix := bdef.mix
	fmt.Println("\tNumClients:", bdef.clients, "Numthreads", bdef.threads,
		"Servers", cluster.servers, "Preconditioning", cluster.precondition, "Operations", bdef.operations)
	fmt.Printf("\tpGet=%v pSet=%v pDel=%v pCAS=%v pAsyncSet=%v pSetNew=%v\n",
		mix.pGet, mix.pSet, mix.pDel, mix.pCAS, mix.pAsyncSet, mix.pSetNew)

	var w sync.WaitGroup
	clients := make([]*client.DBClient, bdef.clients)
	for i := range clients {
		c, err := client.Connect(cluster.addr)
		c.SetBufferingMode(bdef.bufferingMode)
		if err != nil {
			t.Fatal(i, err)
		}
		defer c.Close()
		clients[i] = c
	}
	w.Add(bdef.threads)
	p := tlfmt.NewProgress("\tOperating...", bdef.operations)
	latencies := make([]float64, bdef.operations)
	ops := int32(0)
	runtime.Gosched()
	runtime.GC()
	t1 := time.Now()
	for i := 0; i < bdef.threads; i++ {
		go func(gorID int) {
			c := clients[gorID%bdef.clients]
			key := make([]byte, 4)
			value := make([]byte, 4)
			opID := atomic.AddInt32(&ops, 1)
			for opID < int32(bdef.operations) {
				binary.LittleEndian.PutUint32(key, uint32(rand.Int31n(int32(bdef.space))))
				p.Inc()
				//Operate
				op := rand.Float32()
				//fmt.Println(op, key, value)
				t1 := time.Now()
				switch {
				case op < mix.pGet:
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
				case op < mix.pGet+mix.pSet:
					wr, err := c.Set(key, value)
					if !wr || err != nil {
						fmt.Println(wr, err)
					}
				case op < mix.pGet+mix.pSet+mix.pDel:
					c.Del(key)
				case op < mix.pGet+mix.pSet+mix.pDel+mix.pCAS:
					c.CAS(key, value, time.Time{}, value)
				case op < mix.pGet+mix.pSet+mix.pDel+mix.pCAS+mix.pAsyncSet:
					c.AsyncSet(key, value)
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
			if mix.pCAS > 0 {
				c.Set(key, value)
			}
			w.Done()
		}(i)
	}
	w.Wait()
	t2 := time.Now()
	//Print stats
	fmt.Println("\tThroughput:", float64(bdef.operations)/(t2.Sub(t1).Seconds()), "ops/s", "\tTime:", t2.Sub(t1).Seconds(), "s")
	sort.Float64Slice(latencies).Sort()
	fmt.Println("\t50%", latencies[bdef.operations*50/100]*1000.0, "ms", "\t99%", latencies[bdef.operations*99/100]*1000.0, "ms",
		"\t99.9%", latencies[bdef.operations*999/1000]*1000.0, "ms")

}
