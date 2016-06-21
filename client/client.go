package client

import (
	"encoding/binary"
	"errors"
	"time"
	"github.com/dv343/treeless/com"
	"github.com/dv343/treeless/dist/heartbeat"
	"github.com/dv343/treeless/dist/servergroup"
	"github.com/dv343/treeless/hashing"
)

const defaultGetTimeout = time.Millisecond * 500
const defaultSetTimeout = time.Millisecond * 500
const defaultDelTimeout = time.Millisecond * 500
const defaultCASTimeout = time.Millisecond * 500

//DBClient provides an interface for Treeless client operations
type DBClient struct {
	sg         *servergroup.ServerGroup
	hb         *heartbeat.Heartbeater
	GetTimeout time.Duration
	SetTimeout time.Duration
	DelTimeout time.Duration
	CASTimeout time.Duration
}

//Connect creates a new DBClient and connects it to a Treeless server group by using addr as the entry point
func Connect(addr string) (*DBClient, error) {
	c := new(DBClient)
	sg, err := servergroup.Assoc(addr, "")
	if err != nil {
		return nil, err
	}
	c.sg = sg
	c.hb = heartbeat.Start(sg)
	c.GetTimeout = defaultGetTimeout
	c.SetTimeout = defaultSetTimeout
	c.DelTimeout = defaultDelTimeout
	c.CASTimeout = defaultCASTimeout
	return c, nil
}

//Get return the value associated to a given key
//lastTime is the last modification time of the pair
//read will be true if at least one server respond
func (c *DBClient) Get(key []byte) (value []byte, lastTime time.Time, read bool) {
	//Last write wins policy
	chunkID := hashing.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	var charray [8]com.GetOperation
	var chvalidarray [8]bool
	var times [8]time.Time
	for i, s := range servers {
		if s != nil {
			c, err := s.Get(key, c.GetTimeout)
			if err == nil {
				charray[i] = c
				chvalidarray[i] = true
			}
		}
	}
	read = false
	for i := 0; i < len(servers); i++ {
		if !chvalidarray[i] {
			continue
		}
		r := charray[i].Wait()
		if r.Err != nil {
			continue
		}
		v := r.Value
		read = true
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
		return nil, lastTime, read
	}
	//Read-repair
	for i, s := range servers {
		if s != nil && lastTime.After(times[i]) {
			//Repair
			//log.Println("Read reparing", key, value, lastTime, times[i])
			s.Set(key, value, 0)
		}
	}
	return value[8:], lastTime, read
}

//Set sets a key-value pair by creating a new one or by overwriting a previous value
//written is set to true if at least one server respond without errors
func (c *DBClient) Set(key, value []byte) (written bool, errs error) {
	return c.set(key, value, c.SetTimeout)
}

//AsyncSet is similar to Set, but it asks the server to don't ACK the SET message
//It provides more performance than Set
//However, there is no way to be sure that the key-value pair has been written successfully
func (c *DBClient) AsyncSet(key, value []byte) (errs error) {
	_, errs = c.set(key, value, 0)
	return errs
}

func (c *DBClient) set(key, value []byte, timeout time.Duration) (written bool, errs error) {
	chunkID := hashing.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	valueWithTime := make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(valueWithTime, uint64(time.Now().UnixNano()))
	copy(valueWithTime[8:], value)
	var charray [8]com.SetOperation
	var chvalidarray [8]bool
	for i, s := range servers {
		if s == nil {
			continue
		}
		c, err := s.Set(key, valueWithTime, timeout)
		if err != nil {
			errs = err //TODO return all errors
		} else {
			charray[i] = c
			chvalidarray[i] = true
		}
	}
	if timeout > 0 {
		for i, _ := range servers {
			if chvalidarray[i] {
				err := charray[i].Wait()
				if err != nil {
					errs = err //TODO return all errors
				} else {
					written = true
				}
			}
		}
	}
	return written, errs
}

//CAS (Compare And Swap) modifies the value of a pair if the provided timestamp and old value match the stored value
//This is an atomic operation, but it doesn't tolerate network partitions (permanent node failures are ok)
//It can be used with Get to achieve synchronization when there are no network partitions
//Using it concurrently with Set is a race condition
func (c *DBClient) CAS(key, value []byte, timestamp time.Time, oldValue []byte) (written bool, errs error) {
	chunkID := hashing.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	valueWithTime := make([]byte, 24+len(value))
	casTime := time.Now()
	binary.LittleEndian.PutUint64(valueWithTime[0:8], uint64(timestamp.UnixNano()))
	binary.LittleEndian.PutUint64(valueWithTime[8:16], hashing.FNV1a64(oldValue))
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
			rank[i] = hashing.FNV1a64(b)
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
	op, err := servers[master].CAS(key, valueWithTime, c.CASTimeout)
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

//Del deletes a key-value pair from the DB
//However, if there is a network partition the deleted pair can reappear after the network partition heals
//Setting the value to nil is more safe, but that won't free all memory
func (c *DBClient) Del(key []byte) (errs error) {
	chunkID := hashing.GetChunkID(key, c.sg.NumChunks())
	servers := c.sg.GetChunkHolders(chunkID)
	t := make([]byte, 8)
	binary.LittleEndian.PutUint64(t, uint64(time.Now().UnixNano()))
	var charray [8]*com.DelOperation
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

//SetBuffered activates buffering in all client-server and server-client communications
//Buffering is activated by default
func (c *DBClient) SetBuffered() {
	c.sg.SetBuffered()
}

//SetNoDelay deactivates buffering in all client-server and server-client communications
//Buffering provides a critical performance gain, use this with caution
//Buffering is activated by default
func (c *DBClient) SetNoDelay() {
	c.sg.SetNoDelay()

}

//Close close all connections
func (c *DBClient) Close() {
	//Stop hearbeat
	c.hb.Stop()
	//Close sockets
	c.sg.Stop()
}
