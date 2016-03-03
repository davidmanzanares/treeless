package tlsg

import (
	"encoding/binary"
	"time"
	"treeless/src/com"
	"treeless/src/hash"
)

type DBClient struct {
	sg         *ServerGroup
	GetTimeout time.Duration
	SetTimeout time.Duration
	DelTimeout time.Duration
}

func Connect(addr string) (*DBClient, error) {
	c := new(DBClient)
	sg, err := ConnectAsClient(addr)
	if err != nil {
		return nil, err
	}
	c.sg = sg
	c.GetTimeout = time.Millisecond * 100
	c.SetTimeout = time.Millisecond * 100
	c.DelTimeout = time.Millisecond * 100
	return c, nil
}

func (c *DBClient) Get(key []byte) (value []byte, lastTime time.Time) {
	//Last write wins policy
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	servers := c.sg.GetChunkServers(chunkID)
	var charray [8]chan tlcom.Result
	var vsarray [8]*VirtualServer
	chs := 0

	for _, s := range servers {
		if s == nil {
			continue
		}
		ch := s.Get(key, c.GetTimeout)
		if ch == nil {
			continue
		}
		charray[chs] = ch
		vsarray[chs] = s
		chs++
	}
	for i := 0; i < chs; i++ {
		r := <-charray[i]
		vsarray[i].RUnlock()
		if r.Err != nil {
			vsarray[i].Timeout() //TODO value, err, cerr
			continue
		}
		v := r.Value
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
		err := s.Set(key, valueWithTime, c.SetTimeout)
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
		err := s.Del(key, c.DelTimeout)
		errs = err
	}
	return errs
}

func (c *DBClient) Close() {
	if c.sg != nil {
		c.sg.Stop()
	}
}
