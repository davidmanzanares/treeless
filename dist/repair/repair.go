package repair

import (
	"log"
	"time"
	"treeless/dist/servergroup"
	"treeless/local"
)

var checkInterval = time.Second

func repair(sg *servergroup.ServerGroup, lh *local.Core, cid int) {
	log.Println("Backwards reparing chunk ", cid)
	//Backwards iteration with early exit
	i := 0
	lh.BackwardsIterate(cid, func(key, value []byte) bool {
		if i%128 == 0 && sg.IsSynched(cid) {
			log.Println("Backwards reparing early exit ", cid)
			return false
		}
		servers := sg.GetChunkHolders(cid)
		for _, s := range servers {
			if s == nil {
				continue
			}
			s.Set(key, value, 0)
		}
		return true
	})
	log.Println("Backwards reparing process finished1 ", cid)
}

func StartRepairSystem(sg *servergroup.ServerGroup, lh *local.Core, ShouldStop func() bool) {
	go func() {
		m := make(map[int]int)
		for !ShouldStop() {
			sg.SetServerChunks(lh.LocalhostIPPort, lh.ChunksList())
			for cid := 0; cid < sg.NumChunks(); cid++ {
				if !lh.IsPresent(cid) {
					continue
				}
				if sg.IsSynched(cid) {
					delete(m, cid)
				} else {
					m[cid] = m[cid] + 1
					if m[cid] >= 6 {
						repair(sg, lh, cid)
					}
				}
			}
			time.Sleep(checkInterval)
		}
	}()
}
