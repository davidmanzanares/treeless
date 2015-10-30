package tlcom

import (
	"sort"
)

type virtualChunk struct {
	id           int          //Chunk ID
	allServers   []serverRank //Ordered (by hash weight) list with the servers
	knownServers []*server    //Ordered (by hash weight) list with the servers, only servers with the chunk are included
}

type serverRank struct {
	server *server
	rank   uint64
}

//Access provides an access to a DB server group
type Access struct {
	vChunks   []*virtualChunk
	servers   []*server
	GetPolicy float64
}

type server struct {
	phy         string
	knownChunks []int //Get by heartbeat
}

func (s *server) knowChunk(chunkId int) bool {
	return false
}

func hashServerChunk(serverPhy string, chunkID int) uint64 {
	return 0
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

func CreateAccess(ac *AccessConf) {
	var a Access
	//Establish all server connections

	//Create virtual chunks
	a.vChunks = make([]*virtualChunk, ac.Chunks)
	for chunkID := 0; chunkID < ac.Chunks; chunkID++ {
		a.vChunks[chunkID] = new(virtualChunk)
		a.vChunks[chunkID].id = chunkID
		a.vChunks[chunkID].allServers = make([]serverRank, len(a.servers))
		a.vChunks[chunkID].knownServers = make([]*server, 0)
		//Calculate server ranking for each chunk
		for j := 0; j < len(a.servers); j++ {
			s := a.servers[j]
			rank := hashServerChunk(s.phy, chunkID)
			a.vChunks[chunkID].allServers[j] = serverRank{s, rank}
		}
		//Sort by rank
		sort.Sort(byRank(a.vChunks[chunkID].allServers))
		//for each known, add to knownServers
		for j := 0; j < len(a.vChunks[chunkID].allServers); j++ {
			s := a.vChunks[chunkID].allServers[j].server
			if s.knowChunk(chunkID) {
				a.vChunks[chunkID].knownServers = append(a.vChunks[chunkID].knownServers, s)
			}
		}
	}
}
