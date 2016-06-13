package core

import (
	"log"
	"treeless/core/pmap"
	"treeless/hashing"
)

const defragBufferSize = 256

type defragOp struct {
	chunkID int
	status  chan bool
}

func newDefragmenter(c *Core) chan<- defragOp {
	inputChannel := make(chan defragOp, defragBufferSize)
	go func() {
		for op := range inputChannel {
			chunk := c.chunks[op.chunkID]
			log.Println("Defrag id: ", op.chunkID, " Deleted: ", chunk.pm.Deleted(), " Length: ", chunk.pm.Used())

			chunk.defragMutex.Lock()
			chunk.Lock()

			old := chunk.pm
			chunk.revision++
			if c.dbpath == "" {
				chunk.pm = pmap.New("", c.chunkSize)
			} else {
				chunk.pm = pmap.New(c.chunkPath(op.chunkID, chunk.revision), c.chunkSize)
			}

			old.Iterate(func(key, value []byte) bool {
				h := hashing.FNV1a64(key)
				err := chunk.pm.Set(h, key, value)
				if err != nil {
					panic(err)
				}
				return true
			})
			old.CloseAndDelete()

			chunk.Unlock()
			chunk.defragMutex.Unlock()
			if op.status != nil {
				op.status <- true
			}
		}
	}()
	return inputChannel
}
