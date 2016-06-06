package protocol

import (
	"encoding/binary"
	"errors"
)

const MaxHeartbeatSize = 1400

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
	//KnownChunks
	msg := make([]byte, MaxHeartbeatSize)
	binary.LittleEndian.PutUint16(msg[0:], uint16(len(aa.KnownChunks)))
	m := msg[2:]
	for _, c := range aa.KnownChunks {
		binary.LittleEndian.PutUint32(m[:], uint32(c.ID))
		binary.LittleEndian.PutUint64(m[4:], c.Checksum)
		m = m[12:]
	}

	//RecentlyAddedServers
	for _, addr := range aa.RecentlyAddedServers {
		if len(addr)+64 >= len(m[:]) {
			break
		}
		binary.LittleEndian.PutUint16(m, uint16(len([]byte(addr))))
		copy(m[2:], addr)
		m = m[2+len([]byte(addr)):]
	}

	binary.LittleEndian.PutUint16(m, 0)
	m = m[2:]
	//RecentlyDeadServers
	for _, addr := range aa.RecentlyDeadServers {
		if len(addr)+4 >= len(m[:]) {
			break
		}
		binary.LittleEndian.PutUint16(m[:2], uint16(len([]byte(addr))))
		copy(m[2:], addr)
		m = m[2+len([]byte(addr)):]
	}
	binary.LittleEndian.PutUint16(m, 0)

	return msg
	/*s, err := json.Marshal(aa)
	if err != nil {
		panic(err)
	}
	return s*/
}

//AmAliveUnMarshal unserializes s into an AmAlive object
func AmAliveUnMarshal(msg []byte) (*AmAlive, error) {
	aa := new(AmAlive)
	lenKnownChunks := binary.LittleEndian.Uint16(msg)
	aa.KnownChunks = make([]AmAliveChunk, 0, lenKnownChunks)
	m := msg[2:]
	for i := 0; i < int(lenKnownChunks); i++ {
		if len(m) < 16 {
			return nil, errors.New("Bad formatting, error 1")
		}
		var chunk AmAliveChunk
		chunk.ID = int(binary.LittleEndian.Uint32(m))
		chunk.Checksum = binary.LittleEndian.Uint64(m[4:])
		aa.KnownChunks = append(aa.KnownChunks, chunk)
		m = m[12:]
	}

	for {
		if len(m) < 4 {
			return nil, errors.New("Bad formatting, error 2")
		}
		lenAddr := binary.LittleEndian.Uint16(m)
		if lenAddr == 0 {
			break
		}
		addr := string(m[2 : 2+lenAddr])
		aa.RecentlyAddedServers = append(aa.RecentlyAddedServers, addr)
		m = m[2+lenAddr:]
	}

	for {
		if len(m) < 2 {
			return nil, errors.New("Bad formatting, error 3")
		}
		lenAddr := binary.LittleEndian.Uint16(m)
		if lenAddr == 0 {
			break
		}
		addr := string(m[2 : 2+lenAddr])
		aa.RecentlyDeadServers = append(aa.RecentlyDeadServers, addr)
		m = m[2+lenAddr:]
	}

	return aa, nil
	/*var aa AmAlive
	err := json.Unmarshal(s, &aa)
	return &aa, err*/
}
