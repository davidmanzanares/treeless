package tlsg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
	"treeless/src/com"
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

func (c *DBClient) Set(key, value []byte) error {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	conns := make([]*tlcom.Conn, 0, 4)
	var firstError error
	c.sg.Mutex.Lock()
	/*if rand.Float32() < 0.005 {
		fmt.Println(len(holders))
	}*/
	for _, h := range holders {
		err := h.NeedConnection()
		if err == nil {
			conns = append(conns, h.Conn)
		}
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	c.sg.Mutex.Unlock()
	valueWithTime := make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(valueWithTime, uint64(time.Now().UnixNano()))
	copy(valueWithTime[8:], value)
	for _, c := range conns {
		err := c.Set(key, valueWithTime)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *DBClient) Del(key []byte) error {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	conns := make([]*tlcom.Conn, 0, 4)
	var firstError error
	c.sg.Mutex.Lock()
	for _, h := range holders {
		err := h.NeedConnection()
		if err == nil {
			conns = append(conns, h.Conn)
		}
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	c.sg.Mutex.Unlock()
	for _, c := range conns {
		err := c.Del(key)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *DBClient) Get(key []byte) ([]byte, time.Time, error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	var errs error = nil
	var value []byte = nil
	//Last write wins policy
	lastTime := time.Unix(0, 0)
	for _, h := range holders {
		cerr := h.NeedConnection()
		if cerr != nil {
			if errs == nil {
				errs = errors.New("Holders:" + fmt.Sprint(holders) + "\n" + cerr.Error())
			} else {
				errs = errors.New(errs.Error() + cerr.Error())
			}
			continue
		}
		v, err := h.Conn.Get(key)
		if err == nil {
			if len(v) > 0 {
				t := time.Unix(0, int64(binary.LittleEndian.Uint64(v[:8])))
				if lastTime.Before(t) {
					lastTime = t
					value = v
				}
			}
		} else {
			//h.Conn.Close()
			//h.Conn = nil
			if errs == nil {
				errs = errors.New("Holders:" + fmt.Sprint(holders) + "\n" + err.Error())
			} else {
				errs = errors.New("Multiple errors: " + errs.Error() + ", " + err.Error())
			}
		}
	}
	if value != nil {
		return value[8:], lastTime, errs
	}
	return nil, time.Unix(0, 0), errs
}

func (c *DBClient) Close() {
	if c.sg != nil {
		c.sg.Stop()
	}
}
