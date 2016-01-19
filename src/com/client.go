package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"
	"treeless/src/com/tcp"
)

//Stores a DB operation result
type result struct {
	value []byte
	err   error
}

//ClientConn is a DB TCP client connection
type ClientConn struct {
	conn         net.Conn
	writeChannel chan tlTCP.Message
	waitLock     sync.Mutex
	waits        map[uint32](chan result)
	tid          uint32
	chanPool     sync.Pool
}

const writeTimeWindow = 1000

//CreateConnection returns a new DB connection
func CreateConnection(addr string) (*ClientConn, error) {

	var c ClientConn
	c.chanPool.New = func() interface{} {
		return make(chan result)
	}

	c.waits = make(map[uint32](chan result))
	taddr, errp := net.ResolveTCPAddr("tcp", addr)
	if errp != nil {
		return nil, errp
	}
	//log.Println("Dialing for new connection", taddr)
	tcpconn, err := net.DialTCP("tcp", nil, taddr)
	if err != nil {
		return nil, err
	}
	writeCh := make(chan tlTCP.Message, 128)
	c.conn = tcpconn
	c.writeChannel = writeCh

	go tlTCP.Writer(tcpconn, writeCh)
	go listenToResponses(&c)

	return &c, nil
}

func listenToResponses(c *ClientConn) {
	f := func(m tlTCP.Message) {
		c.waitLock.Lock()
		ch := c.waits[m.ID]
		c.waitLock.Unlock()
		switch m.Type {
		case tlTCP.OpGetResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlTCP.OpGetResponseError:
			ch <- result{nil, errors.New(string(m.Value))}
		case tlTCP.OpGetConfResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlTCP.OpAddServerToGroupACK:
			ch <- result{nil, nil}
		case tlTCP.OpGetChunkInfoResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlTCP.OpOK:
			ch <- result{nil, nil}
		default:
			err := make([]byte, len(m.Value))
			copy(err, m.Value)
			ch <- result{nil, errors.New("Response error: " + string(err))}
		}
	}
	tlTCP.Reader(c.conn, f)
	//log.Println("Connection closed", c.conn.RemoteAddr().String())
}

//Close this connection
func (c *ClientConn) Close() {
	if c != nil && c.conn != nil && c.conn.Close() == nil {
		close(c.writeChannel)
		c.writeChannel = nil
		c.conn = nil
	}
}

//Get the value of  key
func (c *ClientConn) Get(key []byte) ([]byte, error) {
	var mess tlTCP.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpGet
	mess.Key = key
	mess.ID = mytid

	c.writeChannel <- mess

	select {
	case <-time.After(time.Millisecond * 100):
		return nil, errors.New("response timeout")
	case r := <-ch:
		c.chanPool.Put(ch)
		c.waitLock.Lock()
		delete(c.waits, mytid)
		c.waitLock.Unlock()
		return r.value, r.err
	}
}

//Set a new key/value pair
func (c *ClientConn) Set(key, value []byte) error {
	var mess tlTCP.Message

	c.waitLock.Lock()
	mytid := c.tid
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpSet
	mess.ID = mytid
	mess.Key = key
	mess.Value = value

	//fmt.Println("sending put", key, value, len(string(key)), len(key), c.conn.LocalAddr(), c.conn.RemoteAddr())
	c.writeChannel <- mess
	return nil
}

//Transfer a chunk
func (c *ClientConn) Transfer(addr string, chunkID int) error {
	var mess tlTCP.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpTransfer
	mess.ID = mytid
	var err error
	mess.Key, err = json.Marshal(chunkID)
	mess.Value = []byte(addr)
	if err != nil {
		panic(err)
	}

	//fmt.Println("sending put", key, value, len(string(key)), len(key))
	c.writeChannel <- mess

	select {
	case <-time.After(time.Millisecond * 1000000):
		return errors.New("response timeout")
	case r := <-ch:
		c.chanPool.Put(ch)
		c.waitLock.Lock()
		delete(c.waits, mytid)
		c.waitLock.Unlock()
		return r.err
	}
	return nil
}

//GetAccessInfo request DB access info
func (c *ClientConn) GetAccessInfo() ([]byte, error) {
	var mess tlTCP.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpGetConf
	mess.ID = mytid

	c.writeChannel <- mess
	r := <-ch
	c.chanPool.Put(ch)
	c.waitLock.Lock()
	delete(c.waits, mytid)
	c.waitLock.Unlock()
	return r.value, r.err
}

//AddServerToGroup request to add this server to the server group
func (c *ClientConn) AddServerToGroup(addr string) error {
	var mess tlTCP.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpAddServerToGroup
	mess.ID = mytid
	mess.Key = []byte(addr)

	c.writeChannel <- mess
	r := <-ch
	c.chanPool.Put(ch)
	c.waitLock.Lock()
	delete(c.waits, mytid)
	c.waitLock.Unlock()
	return r.err
}

//GetChunkInfo request chunk info
func (c *ClientConn) GetChunkInfo(chunkID int) (size uint64, err error) {
	var mess tlTCP.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlTCP.OpGetChunkInfo
	mess.ID = mytid
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(chunkID))
	mess.Key = b

	c.writeChannel <- mess
	r := <-ch
	c.chanPool.Put(ch)
	c.waitLock.Lock()
	delete(c.waits, mytid)
	c.waitLock.Unlock()
	if r.err != nil {
		return 0, r.err
	}
	size = binary.LittleEndian.Uint64(r.value)
	return size, nil
}
