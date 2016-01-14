package tlclient

import (
	"errors"
	"treeless/src/com"
)

import "treeless/src/hash"

type Client struct {
	sg *tlcom.ServerGroup
}

func Connect(addr string) (*Client, error) {
	c := new(Client)
	sg, err := tlcom.ConnectAsClient(addr)
	if err != nil {
		return nil, err
	}
	c.sg = sg
	return c, nil
}

func (c *Client) Put(key, value []byte) error {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	conns := make([]*tlcom.ClientConn, 0, 4)
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
		err := c.Put(key, value)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *Client) Get(key []byte) ([]byte, error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	var errs error = nil
	var value, v []byte
	for _, h := range holders {
		//TODO get policy
		err := h.NeedConnection()
		if err != nil {
			if errs == nil {
				errs = err
			} else {
				errs = errors.New(errs.Error() + err.Error())
			}
		}

		if err == nil {
			v, err = h.Conn.Get(key)
			if err == nil {
				value = v
			} else {
				if errs == nil {
					errs = err
				} else {
					errs = errors.New("Multiple errors: " + errs.Error() + ", " + err.Error())
				}
			}
		}
	}
	return value, errs
}

func (c *Client) Close() {
	//c.sg.CloseConnections()
}
