package protocol

import "encoding/json"

const MaxShortHeartbeatSize = 1400

type AmAliveChunk struct {
	ID       int
	Checksum uint64
}

//AmAlive stores heartbeat information
type AmAlive struct {
	KnownChunks          []AmAliveChunk //Chunks known by the server
	RecentlyAddedServers []string
	RecentlyDeadServers  []string
}

//Marshal serializes aa into a []byte
func (aa *AmAlive) Marshal() []byte {
	s, err := json.Marshal(aa)
	if err != nil {
		panic(err)
	}
	return s
}

//AmAliveUnMarshal unserializes s into an AmAlive object
func AmAliveUnMarshal(s []byte) (*AmAlive, error) {
	var aa AmAlive
	err := json.Unmarshal(s, &aa)
	return &aa, err
}
