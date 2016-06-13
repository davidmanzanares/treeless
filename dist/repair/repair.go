package repair

import (
	"log"
	"time"
	"treeless/core"
	"treeless/dist/servergroup"
)

var checkInterval = time.Second
var checksTillRepair = 7

func repair(sg *servergroup.ServerGroup, lh *core.Core, cid int) {
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

func StartRepairSystem(sg *servergroup.ServerGroup, lh *core.Core, ShouldStop func() bool) {
	go func() {
		m := make(map[int]int)
		for !ShouldStop() {
			sg.SetServerChunks(sg.LocalhostIPPort, lh.PresentChunksList())
			for cid := 0; cid < sg.NumChunks(); cid++ {
				if !lh.IsPresent(cid) {
					continue
				}
				if sg.IsSynched(cid) {
					delete(m, cid)
				} else {
					m[cid] = m[cid] + 1
					if m[cid] >= checksTillRepair {
						repair(sg, lh, cid)
					}
				}
			}
			time.Sleep(checkInterval)
		}
	}()
}
