package tlcom

import (
	"encoding/json"
	"fmt"
	"time"
)

//Access provides an access to a DB server group
type Access struct {
	Servers []*ServerAccess //List of all servers
	Chunks  []ChunkAccess   //Array of all chunks
}
type ChunkAccess struct {
	//Each chunk has a list of server providers
	Servers []*ServerAccess
}
type ServerAccess struct {
	Phy         string
	Alive       bool
	KnownChunks []int //List of all chunks that this server provides
}

func (a Access) String() string {
	str := fmt.Sprint(len(a.Servers)) + " servers:\n"
	for i := 0; i < len(a.Servers); i++ {
		str += "\t Address: " + a.Servers[i].Phy + "\n\t\tAlive: " +
			fmt.Sprint(a.Servers[i].Alive) + "\n\t\tKnown chunks: " +
			fmt.Sprint(a.Servers[i].KnownChunks) + "\n"
	}
	str += fmt.Sprint(len(a.Chunks)) + " chunks:\n"
	for i := 0; i < len(a.Chunks); i++ {
		srv := ""
		for j := 0; j < len(a.Chunks[i].Servers); j++ {
			srv += "\n\t\t" + a.Chunks[i].Servers[j].Phy
		}
		str += "\tChunk " + fmt.Sprint(i) + srv + "\n"
	}
	return str
}

//CreateAccess will create a new DB access
func CreateAccess(ac *AccessConf) *Access {
	a := new(Access)
	//Create virtual chunks
	a.Chunks = make([]ChunkAccess, ac.Chunks)
	for chunkID := 0; chunkID < ac.Chunks; chunkID++ {
		a.Chunks[chunkID].Servers = make([]*ServerAccess, 0)
	}
	//Establish all server connections
	a.Servers = make([]*ServerAccess, len(ac.Servers))
	for i := 0; i < len(a.Servers); i++ {
		server := new(ServerAccess)
		a.Servers[i] = server
		server.Phy = ac.Servers[i]
		server.KnownChunks = make([]int, 0)
		b, err := UDPRequest(server.Phy, time.Millisecond*50)
		server.Alive = (err == nil)
		if err == nil {
			var udpr UDPResponse
			err = json.Unmarshal(b, &udpr)
			if err != nil {
				panic(err)
			}
			server.KnownChunks = udpr.KnownChunks
			for j := 0; j < len(udpr.KnownChunks); j++ {
				a.Chunks[server.KnownChunks[j]].Servers = append(a.Chunks[server.KnownChunks[j]].Servers, server)
			}
		}
	}
	//Launch UDP heartbeat listener
	fmt.Println(a)
	return a
}

func udpListener() {
	for {
		//listen heartbeats
		//for each new known chunk
		//add server to known chunk list
		//for each forgotten chunk
		//remove server from known chunk list
	}
}

type serverRank struct {
	server *ServerAccess
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
