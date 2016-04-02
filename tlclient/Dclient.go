package tlclient

import (
	"encoding/binary"
	"errors"
	"time"
	"treeless/tlcom"
	"treeless/tlhash"
	"treeless/tlheartbeat"
	"treeless/tlsg"
)

type DBClient struct {
	sg         *tlsg.ServerGroup
	hb         *tlheartbeat.Heartbeater
	GetTimeout time.Duration
	SetTimeout time.Duration
	DelTimeout time.Duration
	drift      time.Duration
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
	//c.drift = time.Duration(time.Nanosecond * time.Duration(rand.Intn(10000*1000)))
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
			//fmt.Println(r.Err)
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
			//log.Println("Read reparing", key, value, lastTime, times[i])
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
	var charray [8]*tlcom.SetOperation
	for i, s := range servers {
		if s == nil {
			continue
		}
		c, err := s.Set(key, valueWithTime, c.SetTimeout)
		if err != nil {
			errs = err //TODO return only written
		} else {
			charray[i] = &c
		}
	}
	if c.SetTimeout > 0 {
		for i, _ := range servers {
			if charray[i] != nil {
				err := charray[i].Wait()
				if err != nil {
					errs = err //TODO return only written
				}
			} else {
				written = true
			}
		}
	}
	return written, errs
}

func (c *DBClient) CAS(key, value []byte, timestamp time.Time, oldValue []byte) (written bool, errs error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	valueWithTime := make([]byte, 24+len(value))
	casTime := time.Now().Add(c.drift)
	binary.LittleEndian.PutUint64(valueWithTime[0:8], uint64(timestamp.UnixNano()))
	binary.LittleEndian.PutUint64(valueWithTime[8:16], tlhash.FNV1a64(oldValue))
	binary.LittleEndian.PutUint64(valueWithTime[16:24], uint64(casTime.UnixNano()))
	copy(valueWithTime[24:], value)
	//Master CAS
	var rank [8]uint64
	for i, s := range servers {
		//Calc rank as hash(key+serverIpPort)
		if s != nil {
			b := make([]byte, len(s.Phy)+len(key))
			copy(b, key)
			copy(b[len(key):], s.Phy)
			rank[i] = tlhash.FNV1a64(b)
			//fmt.Println(s.Phy, rank[i], key)
		}
	}
	master := 0
	masterRank := uint64(0)
	//Select master server
	for i := range servers {
		if rank[i] > masterRank {
			master = i
			masterRank = rank[i]
		}
	}
	//Send CAS
	if servers[master] == nil {
		return false, errors.New("No servers")
	}
	//fmt.Println(servers[master].Phy)
	op, err := servers[master].CAS(key, valueWithTime, c.SetTimeout)
	if err != nil {
		return false, err
	}
	//If CAS won=>set broadcast, else => fail
	err = op.Wait()
	if err != nil {
		return false, err
	}
	//Slave SETs
	for i, s := range servers {
		if s == nil || i == master {
			continue
		}
		_, err := s.Set(key, valueWithTime[16:], 0)
		if err != nil {
			errs = err //TODO return only written
		}
	}
	return true, errs
}

func (c *DBClient) Del(key []byte) (errs error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	t := make([]byte, 8)
	binary.LittleEndian.PutUint64(t, uint64(time.Now().UnixNano()))
	var charray [8]*tlcom.DelOperation
	for i, s := range servers {
		if s == nil {
			continue
		}
		c, err := s.Del(key, t, c.DelTimeout)
		if err != nil {
			//log.Println(1, err)
			errs = err //TODO return only written
		} else {
			charray[i] = &c
		}
	}

	if c.DelTimeout > 0 {
		for i, _ := range servers {
			if charray[i] != nil {
				err := charray[i].Wait()
				if err != nil {
					//log.Println(2, err)
					errs = err //TODO return only written
				}
			}
		}
	}
	return errs
}

func (c *DBClient) Close() {
	//Stop hearbeat
	c.hb.Stop()
	//Stop sockets
	c.sg.Stop()
}
