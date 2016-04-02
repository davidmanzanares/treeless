package tllocals

import (
	"log"
	"treeless/chunk"
	"treeless/tlhash"
)

const defragBufferSize = 256

type defragOp struct {
	chunkID int
	status  chan bool
}

func newDefragmenter(lh *LHStatus) chan<- defragOp {
	inputChannel := make(chan defragOp, defragBufferSize)
	go func() {
		for op := range inputChannel {
			chunk := lh.chunks[op.chunkID]
			log.Println("Defrag id: ", op.chunkID, " Deleted: ", chunk.core.St.Deleted, " Length: ", chunk.core.St.Length)

			chunk.Lock()

			old := chunk.core
			chunk.revision++
			if lh.dbpath == "" {
				chunk.core = tlcore.NewChunk("", lh.size)
			} else {
				chunk.core = tlcore.NewChunk(getChunkPath(lh.dbpath, op.chunkID, chunk.revision), lh.size)
			}

			old.Iterate(func(key, value []byte) bool {
				h := tlhash.FNV1a64(key)
				err := chunk.core.Set(h, key, value)
				if err != nil {
					panic(err)
				}
				return true
			})
			old.CloseAndDelete()

			lh.chunks[op.chunkID].Unlock()
			if op.status != nil {
				op.status <- true
			}
		}
	}()
	return inputChannel
}
