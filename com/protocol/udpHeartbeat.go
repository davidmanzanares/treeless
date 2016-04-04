package protocol

import "encoding/json"

//AmAlive stores heartbeat information
type AmAlive struct {
	KnownChunks  []int    //Chunks known by the server
	KnownServers []string //Servers known by the server
}

//MaxHeartbeatSize is the maximum size of an AmAlive packet
const MaxHeartbeatSize = 1200

//TODO del json

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
