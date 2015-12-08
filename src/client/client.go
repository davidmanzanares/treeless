package tlclient

import "treeless/src/com"
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
	var firstError error
	for h := range holders {
		err := h.NeedConnection()
		if err != nil && firstError == nil {
			firstError = err
		}
		err = h.Conn.Put(key, value)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *Client) Get(key []byte) ([]byte, error) {
	chunkID := tlhash.GetChunkID(key, c.sg.NumChunks)
	holders := c.sg.GetChunkHolders(chunkID)
	var firstError error
	for h := range holders {
		//TODO get policy
		err := h.NeedConnection()
		if err != nil && firstError == nil {
			firstError = err
		}
		value, err2 := h.Conn.Get(key)
		if err2 != nil && firstError == nil {
			firstError = err2
		}
	}
	return firstError
}

func (c *Client) Close() {
	//c.sg.CloseConnections()
}
