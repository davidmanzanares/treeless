package local

import (
	"log"
	"treeless/hashing"
	"treeless/local/pmap"
)

const defragBufferSize = 256

type defragOp struct {
	chunkID int
	status  chan bool
}

func newDefragmenter(lh *Core) chan<- defragOp {
	inputChannel := make(chan defragOp, defragBufferSize)
	go func() {
		for op := range inputChannel {
			chunk := lh.chunks[op.chunkID]
			log.Println("Defrag id: ", op.chunkID, " Deleted: ", chunk.core.Deleted(), " Length: ", chunk.core.Used())

			chunk.defragMutex.Lock()
			chunk.Lock()

			old := chunk.core
			chunk.revision++
			if lh.dbpath == "" {
				chunk.core = pmap.New("", lh.size)
			} else {
				chunk.core = pmap.New(getChunkPath(lh.dbpath, op.chunkID, chunk.revision), lh.size)
			}

			old.Iterate(func(key, value []byte) bool {
				h := hashing.FNV1a64(key)
				err := chunk.core.Set(h, key, value)
				if err != nil {
					panic(err)
				}
				return true
			})
			old.CloseAndDelete()

			lh.chunks[op.chunkID].Unlock()
			chunk.defragMutex.Unlock()
			if op.status != nil {
				op.status <- true
			}
		}
	}()
	return inputChannel
}
