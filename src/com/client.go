package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

//Conn is a DB TCP client connection
type Conn struct {
	conn         net.Conn                 //TCP connection
	writeChannel chan tlTCP.Message       //TCP writer communicattion is made throught this channel
	chanPool     sync.Pool                //Pool of channels to be used as mechanisms to wait until response, make a pool to avoid GC performance penalties
	mutex        sync.Mutex               //Following atribbutes aren't thread-safe, we need to lock and unlock this mutex to protect them
	waits        map[uint32](chan result) //Map of transactions IDs to channels
	tid          uint32                   //Transaction ID
}

//CreateConnection returns a new DB connection
func CreateConnection(addr string) (*Conn, error) {
	//log.Println("Dialing for new connection", taddr)
	taddr, errp := net.ResolveTCPAddr("tcp", addr)
	if errp != nil {
		return nil, errp
	}
	tcpconn, err := net.DialTCP("tcp", nil, taddr)
	if err != nil {
		return nil, err
	}

	var c Conn
	c.conn = tcpconn
	c.chanPool.New = func() interface{} {
		return make(chan result)
	}
	c.waits = make(map[uint32](chan result))
	c.writeChannel = make(chan tlTCP.Message, 128)

	go tlTCP.Writer(tcpconn, c.writeChannel)
	go listenToResponses(&c)

	return &c, nil
}

func listenToResponses(c *Conn) {
	f := func(m tlTCP.Message) {
		c.mutex.Lock()
		ch := c.waits[m.ID]
		c.mutex.Unlock()
		switch m.Type {
		case tlTCP.OpGetResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
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
		case tlTCP.OpTransferCompleted:
			ch <- result{nil, nil}
		case tlTCP.OpErr:
			err := make([]byte, len(m.Value))
			copy(err, m.Value)
			ch <- result{nil, errors.New("Response error: " + string(err))}
		default:
			ch <- result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
		}
	}
	tlTCP.Reader(c.conn, f)
	//log.Println("Connection closed", c.conn.RemoteAddr().String())
}

//Close this connection
func (c *Conn) Close() {
	if c != nil && c.conn != nil && c.conn.Close() == nil {
		close(c.writeChannel)
		c.writeChannel = nil
		c.conn = nil
	}
}

func (c *Conn) getTID() uint32 {
	c.mutex.Lock()
	mytid := c.tid
	c.tid++
	c.mutex.Unlock()
	return mytid
}

func (c *Conn) getTIDChannel() (uint32, chan result) {
	ch := c.chanPool.Get().(chan result)
	c.mutex.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.mutex.Unlock()
	return mytid, ch
}

func (c *Conn) waitForResponse(tid uint32, ch chan result, timeout time.Duration) ([]byte, error) {
	select {
	case <-time.After(timeout):
		c.mutex.Lock()
		delete(c.waits, tid)
		c.mutex.Unlock()
		c.chanPool.Put(ch)
		return nil, errors.New("response timeout")
	case r := <-ch:
		c.mutex.Lock()
		delete(c.waits, tid)
		c.mutex.Unlock()
		c.chanPool.Put(ch)
		return r.value, r.err
	}
}

//Get the value of key
func (c *Conn) Get(key []byte) ([]byte, error) {
	var mess tlTCP.Message

	tid, ch := c.getTIDChannel()

	mess.Type = tlTCP.OpGet
	mess.Key = key
	mess.ID = tid

	c.writeChannel <- mess

	return c.waitForResponse(tid, ch, time.Millisecond*100)
}

//Set a new key/value pair
func (c *Conn) Set(key, value []byte) error {
	var mess tlTCP.Message

	mytid := c.getTID()

	mess.Type = tlTCP.OpSet
	mess.ID = mytid
	mess.Key = key
	mess.Value = value

	//fmt.Println("sending put", key, value, len(string(key)), len(key), c.conn.LocalAddr(), c.conn.RemoteAddr())
	c.writeChannel <- mess
	return nil
}

//Del deletes a key/value pair
func (c *Conn) Del(key []byte) error {
	var mess tlTCP.Message

	mytid := c.getTID()

	mess.Type = tlTCP.OpDel
	mess.ID = mytid
	mess.Key = key

	//fmt.Println("sending del", key, value, len(string(key)), len(key), c.conn.LocalAddr(), c.conn.RemoteAddr())
	c.writeChannel <- mess
	return nil
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	var mess tlTCP.Message

	tid, ch := c.getTIDChannel()

	mess.Type = tlTCP.OpTransfer
	mess.ID = tid
	var err error
	mess.Key, err = json.Marshal(chunkID)
	mess.Value = []byte(addr)
	if err != nil {
		panic(err)
	}

	//fmt.Println("sending put", key, value, len(string(key)), len(key))
	c.writeChannel <- mess

	_, err = c.waitForResponse(tid, ch, time.Millisecond*1000000)
	return err

}

//GetAccessInfo request DB access info
func (c *Conn) GetAccessInfo() ([]byte, error) {
	var mess tlTCP.Message

	tid, ch := c.getTIDChannel()

	mess.Type = tlTCP.OpGetConf
	mess.ID = tid

	c.writeChannel <- mess

	return c.waitForResponse(tid, ch, time.Millisecond*100)
}

//AddServerToGroup request to add this server to the server group
func (c *Conn) AddServerToGroup(addr string) error {
	var mess tlTCP.Message

	tid, ch := c.getTIDChannel()

	mess.Type = tlTCP.OpAddServerToGroup
	mess.ID = tid
	mess.Key = []byte(addr)

	c.writeChannel <- mess

	_, err := c.waitForResponse(tid, ch, time.Millisecond*100)
	return err
}

//GetChunkInfo request chunk info
func (c *Conn) GetChunkInfo(chunkID int) (size uint64, err error) {
	var mess tlTCP.Message

	tid, ch := c.getTIDChannel()

	mess.Type = tlTCP.OpGetChunkInfo
	mess.ID = tid
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(chunkID))
	mess.Key = b

	c.writeChannel <- mess
	rval, err := c.waitForResponse(tid, ch, time.Millisecond*100)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(rval), nil
}
