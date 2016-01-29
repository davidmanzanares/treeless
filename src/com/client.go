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

const tickerReserveSize = 32

//Conn is a DB TCP client connection
type Conn struct {
	conn            net.Conn                 //TCP connection
	writeChannel    chan tlTCP.Message       //TCP writer communicattion is made throught this channel
	chanPool        sync.Pool                //Pool of channels to be used as mechanisms to wait until response, make a pool to avoid GC performance penalties
	mutex           sync.Mutex               //Following atribbutes aren't thread-safe, we need to lock and unlock this mutex to protect them
	waits           map[uint32](chan result) //Map of transactions IDs to channels
	tid             uint32                   //Transaction ID
	isClosed        bool
	tickerReserve   [tickerReserveSize]*time.Ticker
	reservedTickers int
	responseChannel chan ResponserMsg
}

type ResponserMsg struct {
	mess tlTCP.Message
	rch  chan result
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
	c.writeChannel = make(chan tlTCP.Message, 1024)

	c.responseChannel = make(chan ResponserMsg, 1024)

	go tlTCP.Writer(tcpconn, c.writeChannel)
	go Responser(&c)

	return &c, nil
}

func Responser(c *Conn) {
	//TODO: Pointer to Message?
	readChannel := make(chan tlTCP.Message, 1024)
	tid := uint32(0)
	waits := make(map[uint32](chan result)) //TODO array instead of map

	go func() {
		for {
			select {
			case msg := <-c.responseChannel:
				msg.mess.ID = tid
				c.writeChannel <- msg.mess
				waits[tid] = msg.rch
				tid++
			case m := <-readChannel:
				ch := waits[m.ID]
				delete(waits, m.ID)
				switch m.Type {
				case tlTCP.OpGetResponse:
					ch <- result{m.Value, nil}
				case tlTCP.OpSetOK:
					ch <- result{nil, nil}
				case tlTCP.OpDelOK:
					ch <- result{nil, nil}
				case tlTCP.OpGetConfResponse:
					ch <- result{m.Value, nil}
				case tlTCP.OpAddServerToGroupACK:
					ch <- result{nil, nil}
				case tlTCP.OpGetChunkInfoResponse:
					ch <- result{m.Value, nil}
				case tlTCP.OpTransferCompleted:
					ch <- result{nil, nil}
				case tlTCP.OpErr:
					ch <- result{nil, errors.New("Response error: " + string(m.Value))}
				default:
					ch <- result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
				}
			}
		}
	}()
	tlTCP.Reader(c.conn, readChannel)
	//log.Println("Connection closed", c.conn.RemoteAddr().String())
}

//Close this connection
func (c *Conn) Close() {
	if c != nil {
		c.mutex.Lock()
		if c.conn != nil {
			c.conn.Close()
			if !c.isClosed {
				close(c.writeChannel)
			}
		}
		c.isClosed = true
		c.mutex.Unlock()
	}
}

func (c *Conn) IsClosed() bool {
	c.mutex.Lock()
	closed := c.isClosed
	c.mutex.Unlock()
	return closed
}

//TODO timeout
func (c *Conn) sendAndRecieve(opType tlTCP.Operation, key, value []byte) result {
	var m ResponserMsg
	m.mess.Type = opType
	m.mess.Key = key
	m.mess.Value = value
	m.rch = c.chanPool.Get().(chan result)
	c.responseChannel <- m
	r := <-m.rch
	c.chanPool.Put(m.rch)
	return r
}

//Get the value of key
func (c *Conn) Get(key []byte) ([]byte, error) {
	r := c.sendAndRecieve(tlTCP.OpGet, key, nil)
	return r.value, r.err
}

//Set a new key/value pair
func (c *Conn) Set(key, value []byte) error {
	r := c.sendAndRecieve(tlTCP.OpSet, key, value)
	return r.err
}

//Del deletes a key/value pair
func (c *Conn) Del(key []byte) error {
	r := c.sendAndRecieve(tlTCP.OpDel, key, nil)
	return r.err
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	key, err := json.Marshal(chunkID)
	if err != nil {
		panic(err)
	}
	value := []byte(addr)
	r := c.sendAndRecieve(tlTCP.OpTransfer, key, value)
	return r.err
}

//GetAccessInfo request DB access info
func (c *Conn) GetAccessInfo() ([]byte, error) {
	r := c.sendAndRecieve(tlTCP.OpGetConf, nil, nil)
	return r.value, r.err
}

//AddServerToGroup request to add this server to the server group
func (c *Conn) AddServerToGroup(addr string) error {
	key := []byte(addr)
	r := c.sendAndRecieve(tlTCP.OpAddServerToGroup, key, nil)
	return r.err
}

//GetChunkInfo request chunk info
func (c *Conn) GetChunkInfo(chunkID int) (size uint64, err error) {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndRecieve(tlTCP.OpGetChunkInfo, key, nil)
	if r.err != nil {
		return 0, r.err
	}
	return binary.LittleEndian.Uint64(r.value), nil
}
