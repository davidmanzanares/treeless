package tlclient

import (
	"encoding/binary"
	"log"
	"time"
	"treeless/src/tlcom"
	"treeless/src/tlhash"
	"treeless/src/tlheartbeat"
	"treeless/src/tlsg"
)

type DBClient struct {
	sg         *tlsg.ServerGroup
	hb         *tlheartbeat.Heartbeater
	GetTimeout time.Duration
	SetTimeout time.Duration
	DelTimeout time.Duration
}

func Connect(addr string) (*DBClient, error) {
	c := new(DBClient)
	sg, err := tlsg.Assoc(addr)
	if err != nil {
		return nil, err
	}
	c.sg = sg

	//Start heartbeat listener
	c.hb = tlheartbeat.Start(sg)

	c.GetTimeout = time.Millisecond * 500
	c.SetTimeout = time.Millisecond * 500
	c.DelTimeout = time.Millisecond * 500
	return c, nil
}

func (c *DBClient) Get(key []byte) (value []byte, lastTime time.Time) {
	//Last write wins policy
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	var charray [8]*tlcom.GetOperation
	var times [8]time.Time
	for i, s := range servers {
		if s != nil {
			c, err := s.Get(key, c.GetTimeout)
			if err == nil {
				charray[i] = &c
			}
		}
	}
	for i := 0; i < len(servers); i++ {
		if charray[i] == nil {
			continue
		}
		r := charray[i].Wait()
		if r.Err != nil {
			continue
		}
		v := r.Value
		if len(v) >= 8 {
			t := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
			times[i] = t
			if lastTime.Before(t) {
				lastTime = t
				value = v
			}
		}
	}
	if value == nil {
		return nil, lastTime
	}
	//Read-repair
	for i, s := range servers {
		if s != nil && lastTime.After(times[i]) {
			//Repair
			log.Println("Read reparing", key, value, lastTime, times[i])
			s.Set(key, value, 0)
		}
	}
	return value[8:], lastTime
}

func (c *DBClient) Set(key, value []byte) (written bool, errs error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
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
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	valueTime := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueTime, uint64(time.Now().UnixNano()))
	for _, s := range servers {
		if s == nil {
			continue
		}
		err := s.Del(key, valueTime, c.DelTimeout)
		errs = err
	}
	return errs
}

func (c *DBClient) Close() {
	//Stop hearbeat
	c.hb.Stop()
	//Stop sockets
	c.sg.Stop()
}
