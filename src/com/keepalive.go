package tlcom

//Keepalive represents UDP Keepalive packets

type Keepalive struct {
	KnownChunks  []int    //Chunks known by the server
	KnownServers []string //Servers known by the server in format ip:port
}
