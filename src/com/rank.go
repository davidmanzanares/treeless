package tlcom

/*
type serverRank struct {
	server *VirtualServer
	rank   uint64
}
type byRank []serverRank

func (a byRank) Len() int {
	return len(a)
}
func (a byRank) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a byRank) Less(i, j int) bool {
	return a[i].rank < a[j].rank
}

type serverChunk struct {
	ranks []serverRank
}
type serverChunkList []serverChunk

func rebalance(thisServer *ServerAccess, chunks serverChunkList, chunkId int, mutex *sync.RWMutex) {
	mutex.RLock()
	chunk := chunks[chunkId]
	for i := 0; i < len(chunk.ranks); i++ {
		if chunk.ranks[i].server == thisServer {
			mutex.RUnlock()
			time.Sleep(time.Second * 5 * time.Duration(i))
			mutex.RLock()
			//if still not known by other server:
			//	download chunk
			mutex.RUnlock()
			return
		}
	}
	mutex.RUnlock()
}
*/
