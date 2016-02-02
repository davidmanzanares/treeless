package tlcom

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
	"treeless/src/com/proto"
)

//Stores a DB operation result
type result struct {
	value []byte
	err   error
}

const tickerReserveSize = 32

//Conn is a DB TCP client connection
type Conn struct {
	conn            net.Conn  //TCP connection
	chanPool        sync.Pool //Pool of channels to be used as mechanisms to wait until response, make a pool to avoid GC performance penalties
	tickerReserve   [tickerReserveSize]*time.Ticker
	reservedTickers int
	responseChannel chan ResponserMsg
}

type ResponserMsg struct {
	mess    tlproto.Message
	timeout time.Duration
	rch     chan result
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

	c.responseChannel = make(chan ResponserMsg, 1024)

	go broker(&c)

	return &c, nil
}

type timeoutMsg struct {
	t   time.Time
	tid uint32
}
type waiter struct {
	r  chan<- result
	el *list.Element
}

func broker(c *Conn) {
	readChannel := make(chan tlproto.Message, 1024)
	writeChannel := tlproto.NewBufferedConn(c.conn, readChannel)
	waits := make(map[uint32]waiter)
	tid := uint32(0)
	l := list.New() //TODO use array
	ticker := time.NewTicker(time.Millisecond * 10)
	go func() {
		for {
			select {
			case now := <-ticker.C:
				for l.Len() > 0 {
					el := l.Front()
					f := el.Value.(timeoutMsg)
					if now.After(f.t) {
						//Timeout'ed
						w := waits[f.tid]
						delete(waits, f.tid)
						w.r <- result{nil, errors.New("Timeout")}
						l.Remove(el)
					} else {
						break
					}
				}
			case msg, ok := <-c.responseChannel:
				if !ok {
					if l.Len() > 0 {
						panic("Logic broken")
					}
					close(writeChannel)
					return
				}
				msg.mess.ID = tid
				//TODO: opt dont call time.now
				tm := timeoutMsg{t: time.Now().Add(msg.timeout), tid: tid}
				var inserted *list.Element
				for el := l.Back(); el != l.Front(); el = el.Prev() {
					t := el.Value.(timeoutMsg).t
					if t.Before(tm.t) {
						inserted = l.InsertAfter(tm, el)
						break
					}
				}
				if inserted == nil {
					inserted = l.PushFront(tm)
				}

				waits[tid] = waiter{r: msg.rch, el: inserted}
				tid++
				writeChannel <- msg.mess
			case m := <-readChannel:
				w, ok := waits[m.ID]
				if !ok {
					//Was timeout'ed
					continue
				}
				ch := w.r
				delete(waits, m.ID)
				l.Remove(w.el)
				switch m.Type {
				case tlproto.OpGetResponse:
					ch <- result{m.Value, nil}
				case tlproto.OpSetOK:
					ch <- result{nil, nil}
				case tlproto.OpDelOK:
					ch <- result{nil, nil}
				case tlproto.OpGetConfResponse:
					ch <- result{m.Value, nil}
				case tlproto.OpAddServerToGroupACK:
					ch <- result{nil, nil}
				case tlproto.OpGetChunkInfoResponse:
					ch <- result{m.Value, nil}
				case tlproto.OpTransferCompleted:
					ch <- result{nil, nil}
				case tlproto.OpErr:
					ch <- result{nil, errors.New("Response error: " + string(m.Value))}
				default:
					ch <- result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
				}
			}
		}
	}()
}

//Close this connection
func (c *Conn) Close() {
	close(c.responseChannel)
	c.conn.Close()
}

//TODO timeout
func (c *Conn) sendAndReceive(opType tlproto.Operation, key, value []byte, timeout time.Duration) result {
	var m ResponserMsg
	m.mess.Type = opType
	m.mess.Key = key
	m.mess.Value = value
	m.timeout = timeout
	m.rch = make(chan result) //c.chanPool.Get().(chan result)
	c.responseChannel <- m
	r := <-m.rch
	c.chanPool.Put(m.rch)
	return r
}

//Get the value of key
func (c *Conn) Get(key []byte) ([]byte, error) {
	r := c.sendAndReceive(tlproto.OpGet, key, nil, 100*time.Millisecond)
	return r.value, r.err
}

//Set a new key/value pair
func (c *Conn) Set(key, value []byte) error {
	r := c.sendAndReceive(tlproto.OpSet, key, value, 100*time.Millisecond)
	return r.err
}

//Del deletes a key/value pair
func (c *Conn) Del(key []byte) error {
	r := c.sendAndReceive(tlproto.OpDel, key, nil, 100*time.Millisecond)
	return r.err
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	key, err := json.Marshal(chunkID)
	if err != nil {
		panic(err)
	}
	value := []byte(addr)
	r := c.sendAndReceive(tlproto.OpTransfer, key, value, 5*time.Second)
	return r.err
}

//GetAccessInfo request DB access info
func (c *Conn) GetAccessInfo() ([]byte, error) {
	r := c.sendAndReceive(tlproto.OpGetConf, nil, nil, 500*time.Millisecond)
	return r.value, r.err
}

//AddServerToGroup request to add this server to the server group
func (c *Conn) AddServerToGroup(addr string) error {
	key := []byte(addr)
	r := c.sendAndReceive(tlproto.OpAddServerToGroup, key, nil, 500*time.Millisecond)
	return r.err
}

//GetChunkInfo request chunk info
func (c *Conn) GetChunkInfo(chunkID int) (size uint64, err error) {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndReceive(tlproto.OpGetChunkInfo, key, nil, 500*time.Millisecond)
	if r.err != nil {
		return 0, r.err
	}
	return binary.LittleEndian.Uint64(r.value), nil
}
