package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
	"treeless/src/tlcom/tlproto"
)

//Stores a DB operation result
type Result struct {
	Value []byte
	Err   error
}

const tickerReserveSize = 32

//Conn is a DB TCP client connection
type Conn struct {
	conn            *net.TCPConn
	tickerReserve   [tickerReserveSize]*time.Ticker
	reservedTickers int
	responseChannel chan ResponserMsg
}

type ResponserMsg struct {
	mess    tlproto.Message
	timeout time.Duration
	rch     chan Result
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

	c.responseChannel = make(chan ResponserMsg, 1024)

	go broker(&c)

	return &c, nil
}

type timeoutMsg struct {
	t   time.Time
	tid uint32
}
type waiter struct {
	r chan<- Result
}

func broker(c *Conn) {
	readChannel := make(chan tlproto.Message, 1024)
	writeChannel := tlproto.NewBufferedConn(c.conn, readChannel)
	waits := make(map[uint32]waiter)
	tid := uint32(0)
	pq := make([]timeoutMsg, 0, 64)
	ticker := time.NewTicker(time.Millisecond * 10)
	go func() {
		for {
			select {
			case now := <-ticker.C:
				for len(pq) > 0 {
					f := pq[0]
					if now.After(f.t) {
						//Timeout'ed
						w, ok := waits[f.tid]
						if ok {
							delete(waits, f.tid)
							w.r <- Result{nil, errors.New("Timeout" + fmt.Sprint("Local", c.conn.LocalAddr(), "Remote", c.conn.RemoteAddr()))}
						}
						pq = pq[1:]
					} else {
						break
					}
				}
			case msg, ok := <-c.responseChannel:
				if !ok {
					for len(pq) > 0 {
						f := pq[0]
						w := waits[f.tid]
						delete(waits, f.tid)
						w.r <- Result{nil, errors.New("Connection closed" + fmt.Sprint("Local", c.conn.LocalAddr(), "Remote", c.conn.RemoteAddr()))}
						pq = pq[1:]
					}
					close(writeChannel)
					return
				}
				msg.mess.ID = tid
				if msg.timeout > 0 {
					//Send and *Recieve*
					tm := timeoutMsg{t: time.Now().Add(msg.timeout), tid: tid}

					inserted := false
					for i := len(pq) - 1; i >= 0; i-- {
						t := pq[i].t
						if t.Before(tm.t) {
							pq = append(pq, tm)
							copy(pq[i+2:], pq[i+1:])
							pq[i+1] = tm
							inserted = true
							break
						}
					}
					if inserted == false {
						pq = append(pq, tm)
						copy(pq[0:], pq[1:])
						pq[0] = tm
					}
					waits[tid] = waiter{r: msg.rch}
				}
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
				//l.Remove(w.el)
				switch m.Type {
				case tlproto.OpGetResponse:
					ch <- Result{m.Value, nil}
				case tlproto.OpSetOK:
					ch <- Result{nil, nil}
				case tlproto.OpDelOK:
					ch <- Result{nil, nil}
				case tlproto.OpProtectOK:
					ch <- Result{nil, nil}
				case tlproto.OpGetConfResponse:
					ch <- Result{m.Value, nil}
				case tlproto.OpAddServerToGroupACK:
					ch <- Result{nil, nil}
				case tlproto.OpGetChunkInfoResponse:
					ch <- Result{m.Value, nil}
				case tlproto.OpTransferCompleted:
					ch <- Result{nil, nil}
				case tlproto.OpErr:
					ch <- Result{nil, errors.New("Response error: " + string(m.Value))}
				default:
					ch <- Result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
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

func (c *Conn) send(opType tlproto.Operation, key, value []byte,
	timeout time.Duration) chan Result {
	if timeout == 0 {
		//Send-only
		var m ResponserMsg
		m.mess.Type = opType
		m.mess.Key = key
		m.mess.Value = value
		c.responseChannel <- m
		return nil
	}
	//Send and recieve
	var m ResponserMsg
	m.mess.Type = opType
	m.mess.Key = key
	m.mess.Value = value
	m.timeout = timeout
	m.rch = make(chan Result, 1)
	c.responseChannel <- m
	return m.rch
}

func (c *Conn) sendAndReceive(opType tlproto.Operation, key, value []byte,
	timeout time.Duration) Result {
	rch := c.send(opType, key, value, timeout)
	if rch == nil {
		return Result{}
	}
	return <-rch
}

//Get the value of key
func (c *Conn) Get(key []byte, timeout time.Duration) chan Result {
	if timeout <= 0 {
		panic("get timeout <=0")
	}
	x := c.send(tlproto.OpGet, key, nil, timeout)
	if x == nil {
		panic("xnil")
	}
	return x
}

//Set a new key/value pair
func (c *Conn) Set(key, value []byte, timeout time.Duration) error {
	r := c.sendAndReceive(tlproto.OpSet, key, value, timeout)
	return r.Err
}

//Del deletes a key/value pair
func (c *Conn) Del(key []byte, timeout time.Duration) error {
	r := c.sendAndReceive(tlproto.OpDel, key, nil, timeout)
	return r.Err
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	key, err := json.Marshal(chunkID)
	if err != nil {
		panic(err)
	}
	value := []byte(addr)
	r := c.sendAndReceive(tlproto.OpTransfer, key, value, 500*time.Millisecond)
	return r.Err
}

//GetAccessInfo request DB access info
func (c *Conn) GetAccessInfo() ([]byte, error) {
	r := c.sendAndReceive(tlproto.OpGetConf, nil, nil, 500*time.Millisecond)
	return r.Value, r.Err
}

//AddServerToGroup request to add this server to the server group
func (c *Conn) AddServerToGroup(addr string) error {
	key := []byte(addr)
	r := c.sendAndReceive(tlproto.OpAddServerToGroup, key, nil, 500*time.Millisecond)
	return r.Err
}

func (c *Conn) Protect(chunkID int) error {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndReceive(tlproto.OpProtect, key, nil, 500*time.Millisecond)
	if r.Err != nil {
		return r.Err
	}
	return nil
}

//GetChunkInfo request chunk info
func (c *Conn) GetChunkInfo(chunkID int) (size uint64, err error) {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndReceive(tlproto.OpGetChunkInfo, key, nil, 500*time.Millisecond)
	if r.Err != nil {
		return 0, r.Err
	}
	return binary.LittleEndian.Uint64(r.Value), nil
}
