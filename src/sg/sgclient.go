package tlsg

import (
	"encoding/binary"
	"time"
	"treeless/src/hash"
)

type DBClient struct {
	sg *ServerGroup
}

func Connect(addr string) (*DBClient, error) {
	c := new(DBClient)
	sg, err := ConnectAsClient(addr)
	if err != nil {
		return nil, err
	}
	c.sg = sg
	return c, nil
}

func (c *DBClient) Get(key []byte) (value []byte, lastTime time.Time) {
	//Last write wins policy
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	servers := c.sg.GetChunkServers(chunkID)
	for _, s := range servers {
		if s == nil {
			continue
		}
		v := s.Get(key)
		if len(v) >= 8 {
			t := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
			if lastTime.Before(t) {
				lastTime = t
				value = v
			}
		}
	}
	if value != nil {
		return value[8:], lastTime
	}
	return nil, lastTime
}

func (c *DBClient) Set(key, value []byte) (written bool, errs error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	servers := c.sg.GetChunkServers(chunkID)
	valueWithTime := make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(valueWithTime, uint64(time.Now().UnixNano()))
	copy(valueWithTime[8:], value)
	for _, s := range servers {
		if s == nil {
			continue
		}
		err := s.Set(key, valueWithTime)
		if err == nil {
			written = true
		} else {
			errs = err //TODO return only written
		}
	}
	return written, errs
}

func (c *DBClient) Del(key []byte) (errs error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	servers := c.sg.GetChunkServers(chunkID)
	for _, s := range servers {
		if s == nil {
			continue
		}
		err := s.Del(key)
		errs = err
	}
	return errs
}

func (c *DBClient) Close() {
	if c.sg != nil {
		c.sg.Stop()
	}
}
