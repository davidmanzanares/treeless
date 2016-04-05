package protocol

import (
	"encoding/json"
	"treeless/hashing"
)

const MaxShortHeartbeatSize = 128

//AmAlive stores heartbeat information
type AmAlive struct {
	KnownChunks  []int    //Chunks known by the server
	KnownServers []string //Servers known by the server
}

//ShortAmAlive stores the hash of some heartbeat information
type ShortAmAlive struct {
	AAHash uint64
}

//TODO del json

func (aa *AmAlive) Short() ShortAmAlive {
	s, err := json.Marshal(aa)
	if err != nil {
		panic(err)
	}
	return ShortAmAlive{AAHash: hashing.FNV1a64(s)}
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

//Marshal serializes saa into a []byte
func (saa *ShortAmAlive) Marshal() []byte {
	s, err := json.Marshal(saa)
	if err != nil {
		panic(err)
	}
	return s
}

//ShortAmAliveUnMarshal unserializes s into an ShortAmAlive object
func ShortAmAliveUnMarshal(s []byte) (*ShortAmAlive, error) {
	var aa ShortAmAlive
	err := json.Unmarshal(s, &aa)
	return &aa, err
}
