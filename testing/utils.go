package test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"time"
	"treeless/client"
)

type capability int

const (
	capKill = iota
	capRestart
	capDisconnect
	capReconnect
)

type testServer interface {
	//Node address
	addr() string
	close()

	//For node failure simulation
	create(numChunks, redundancy int, verbose bool, open bool) string
	assoc(addr string, verbose bool, open bool) string
	kill()
	//For network failure simulation
	disconnect()
	reconnect()

	//Each test will need only some of these capabilities, testServers may no implement all
	testCapability(c capability) bool
}

func waitForServer(addr string) bool {
	for i := 0; i < 50; i++ {
		time.Sleep(time.Millisecond * 50)
		client, err := client.Connect(addr)
		if err == nil {
			client.Close()
			return true
		}
	}
	return false
}

func randKVOpGenerator(minKeySize, maxKeySize, maxValueSize, seed, mult, offset int) func() (op int, k, v []byte) {
	r := rand.New(rand.NewSource(int64(seed)))
	base := make([]byte, 4)
	base2 := make([]byte, 4)
	return func() (op int, k, v []byte) {
		opKeySize := r.Intn(maxKeySize) + minKeySize
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
