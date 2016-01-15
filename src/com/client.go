package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"treeless/src/com/lowcom"
)

//Stores a DB operation result
type result struct {
	value []byte
	err   error
}

//ClientConn is a DB TCP client connection
type ClientConn struct {
	conn         net.Conn
	writeChannel chan tlLowCom.Message
	waitLock     sync.Mutex
	waits        map[uint32](chan result)
	tid          uint32
	chanPool     sync.Pool
	closed       int32
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
	writeCh := make(chan tlLowCom.Message, 128)
	c.conn = tcpconn
	c.writeChannel = writeCh

	go tlLowCom.TCPWriter(tcpconn, writeCh)
	go listenToResponses(&c)

	return &c, nil
}

func listenToResponses(c *ClientConn) {
	f := func(m tlLowCom.Message) {
		c.waitLock.Lock()
		ch := c.waits[m.ID]
		c.waitLock.Unlock()
		switch m.Type {
		case tlLowCom.OpGetResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlLowCom.OpGetResponseError:
			ch <- result{nil, errors.New(string(m.Value))}
		case tlLowCom.OpGetConfResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlLowCom.OpAddServerToGroupACK:
			ch <- result{nil, nil}
		case tlLowCom.OpGetChunkInfoResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlLowCom.OpOK:
			ch <- result{nil, nil}
		default:
			err := make([]byte, len(m.Value))
			copy(err, m.Value)
			ch <- result{nil, errors.New("Response error: " + string(err))}
		}
	}
	tlLowCom.TCPReader(c.conn, f)
	//log.Println("Connection closed", c.conn.RemoteAddr().String())
}

func (c *ClientConn) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

//Close this connection
func (c *ClientConn) Close() {
	if !c.isClosed() {
		atomic.StoreInt32(&c.closed, 1)
		close(c.writeChannel)
		c.conn.Close()
	}
}

//Get the value of  key
func (c *ClientConn) Get(key []byte) ([]byte, error) {
	var mess tlLowCom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpGet
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
	var mess tlLowCom.Message

	c.waitLock.Lock()
	mytid := c.tid
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpSet
	mess.ID = mytid
	mess.Key = key
	mess.Value = value

	//fmt.Println("sending put", key, value, len(string(key)), len(key), c.conn.LocalAddr(), c.conn.RemoteAddr())
	c.writeChannel <- mess
	return nil
}

//Transfer a chunk
func (c *ClientConn) Transfer(addr string, chunkID int) error {
	var mess tlLowCom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpTransfer
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
	var mess tlLowCom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpGetConf
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
	var mess tlLowCom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpAddServerToGroup
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
	var mess tlLowCom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlLowCom.OpGetChunkInfo
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
