package tlcom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
	"treeless/com/buffconn"
	"treeless/com/protocol"
)

var brokerTick = time.Millisecond * 5

//Stores a DB operation result
type Result struct {
	Value []byte
	Err   error
}

//Conn is a DB TCP client connection
type Conn struct {
	conn            *net.TCPConn
	responseChannel chan ResponserMsg
	rchPool         sync.Pool
}

type ResponserMsg struct {
	mess    protocol.Message
	timeout time.Duration
	rch     chan Result
}

//CreateConnection returns a new DB connection
func CreateConnection(addr string, onClose func()) (*Conn, error) {
	//log.Println("Dialing for new connection", taddr)
	/*taddr, errp := net.ResolveTCPAddr("tcp", addr)
	if errp != nil {
		return nil, errp
	}*/

	d := &net.Dialer{Timeout: 3 * time.Second}
	tcpconn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	var c Conn
	c.conn = tcpconn.(*net.TCPConn)
	//c.conn.SetNoDelay(false)

	c.responseChannel = make(chan ResponserMsg, 1024)
	c.rchPool = sync.Pool{New: func() interface{} {
		return make(chan Result, 1)
	}}
	go broker(&c, onClose)

	return &c, nil
}

type timeoutMsg struct {
	t          time.Time
	tid        uint32
	prev, next *timeoutMsg
}

type queue struct {
	head, tail *timeoutMsg
	pool       sync.Pool
	l          int
}

func createQueue() (q *queue) {
	q = new(queue)
	q.pool = sync.Pool{New: func() interface{} {
		return new(timeoutMsg)
	}}
	return q
}
func (q *queue) push(tm *timeoutMsg) {
	q.l = q.l + 1
	if q.l == 1 {
		q.tail = tm
		q.head = tm
		return
	}
	x := q.tail
	for x.next != nil && x.t.After(tm.t) {
		x = x.next
	}
	if x.next == nil && x.t.After(tm.t) {
		//To head
		q.head = tm
		x.next = tm
		tm.prev = x
	}
	prev := x.prev
	x.prev = tm
	tm.next = x
	tm.prev = prev
	if prev != nil {
		prev.next = tm
	} else {
		q.tail = tm
	}
}
func (q *queue) len() int {
	return q.l
}
func (q *queue) clean(now time.Time, timeoutCallback func(tm *timeoutMsg)) {
	for q.l > 0 {
		f := q.head
		if now.After(f.t) {
			//Timeout'ed
			q.head = f.prev
			q.l = q.l - 1
			if q.l == 1 {
				q.head.next = nil
				q.tail = q.head
			} else if q.l > 1 {
				q.head.next = nil
			} else {
				q.head = nil
				q.tail = nil
			}
			f.next = nil
			f.prev = nil
			q.pool.Put(f)
			timeoutCallback(f)
		} else {
			break
		}
	}
}
func (q *queue) forall(callback func(tm *timeoutMsg)) {
	for q.l > 0 {
		f := q.head
		callback(f)
		q.head = f.prev
		f.next = nil
		f.prev = nil
		q.pool.Put(f)
		q.l--
	}
	q.head = nil
	q.tail = nil
}

func broker(c *Conn, onClose func()) {
	bconn := buffconn.New(c.conn)
	waits := make(map[uint32]chan<- Result)
	pq := createQueue()
	tickerActivation := make(chan bool)
	var mutex sync.Mutex
	go func() {
		//Ticker
		ticker := time.NewTicker(brokerTick)
		defer ticker.Stop()
		activated := false
		for {
			if activated {
				now := <-ticker.C
				mutex.Lock()
				pq.clean(now, func(tm *timeoutMsg) {
					w, ok := waits[tm.tid]
					if ok {
						delete(waits, tm.tid)
						w <- Result{nil, errors.New("Timeout" + fmt.Sprint("Local", c.conn.LocalAddr(), "Remote", c.conn.RemoteAddr()))}
					}
				})
				if pq.len() == 0 {
					activated = false
				}
				mutex.Unlock()
			} else {
				var ok bool
				activated, ok = <-tickerActivation
				if !ok {
					return
				}
			}
		}
	}()

	go func() {
		//To world
		tid := uint32(0)
		for {
			msg, ok := <-c.responseChannel
			mutex.Lock()
			if !ok {
				pq.forall(func(tm *timeoutMsg) {
					w, ok := waits[tm.tid]
					if ok {
						if w == nil {
							panic("w rch == nil")
						}
						delete(waits, tm.tid)
						w <- Result{nil, errors.New("Connection closed => fast timeout" + fmt.Sprint("Local", c.conn.LocalAddr(), "Remote", c.conn.RemoteAddr()))}
					}
				})
				bconn.Close()
				mutex.Unlock()
				return
			}
			msg.mess.ID = tid
			if msg.timeout > 0 {
				//Send and *Receive*
				if msg.rch == nil {
					panic("msg rch == nil")
				}
				tm := pq.pool.Get().(*timeoutMsg)
				tm.t = time.Now().Add(msg.timeout)
				tm.tid = tid

				pq.push(tm)
				waits[tid] = msg.rch
			} else {
				if msg.rch != nil {
					panic("msg rch != nil")
				}
			}
			mutex.Unlock()
			bconn.Write(msg.mess)
			tid++
			if msg.timeout > 0 {
				select {
				case tickerActivation <- true:
				default:
				}
			}
		}
	}()

	go func() {
		//From world
		for {
			m, err := bconn.Read()
			mutex.Lock()
			if err != nil {
				//Connection closed
				mutex.Unlock()
				onClose()
				return
			}
			w, ok := waits[m.ID]
			if !ok {
				//Was timeout'ed
				mutex.Unlock()
				continue
			}
			ch := w
			delete(waits, m.ID)
			mutex.Unlock()
			switch m.Type {
			case protocol.OpResponse:
				ch <- Result{m.Value, nil}
			case protocol.OpOK:
				ch <- Result{nil, nil}
			case protocol.OpErr:
				ch <- Result{nil, errors.New("Response error: " + string(m.Value))}
			default:
				ch <- Result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
			}
		}
	}()
}

//Close this connection
func (c *Conn) Close() {
	close(c.responseChannel)
}

func (c *Conn) send(opType protocol.Operation, key, value []byte,
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
	m.rch = c.rchPool.Get().(chan Result)
	c.responseChannel <- m
	return m.rch
}

func (c *Conn) sendAndReceive(opType protocol.Operation, key, value []byte,
	timeout time.Duration) Result {
	rch := c.send(opType, key, value, timeout)
	if rch == nil {
		return Result{}
	}
	return <-rch
}

type GetOperation struct {
	rch chan Result
	c   *Conn
}

type SetOperation struct {
	rch chan Result
	c   *Conn
}

type DelOperation struct {
	rch chan Result
	c   *Conn
}

type CASOperation struct {
	rch chan Result
	c   *Conn
}

func (g *GetOperation) Wait() Result {
	if g.rch == nil {
		return Result{nil, errors.New("Already returned")}
	}
	r := <-g.rch
	g.c.rchPool.Put(g.rch)
	g.rch = nil
	return r
}

func (g *SetOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.rchPool.Put(g.rch)
	g.rch = nil
	return r.Err
}

func (g *DelOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.rchPool.Put(g.rch)
	g.rch = nil
	return r.Err
}

func (g *CASOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.rchPool.Put(g.rch)
	g.rch = nil
	return r.Err
}

//Get the value of key
func (c *Conn) Get(key []byte, timeout time.Duration) GetOperation {
	if timeout <= 0 {
		panic("get timeout <=0")
	}
	ch := c.send(protocol.OpGet, key, nil, timeout)
	if ch == nil {
		panic("xnil")
	}
	return GetOperation{rch: ch, c: c}
}

//Set a new key/value pair
func (c *Conn) Set(key []byte, value []byte, timeout time.Duration) SetOperation {
	op := protocol.OpSet
	if timeout == 0 {
		op = protocol.OpSetAsync
	}
	ch := c.send(op, key, value, timeout)
	return SetOperation{rch: ch, c: c}
}

//Del deletes a key/value pair
func (c *Conn) Del(key []byte, value []byte, timeout time.Duration) DelOperation {
	ch := c.send(protocol.OpDel, key, value, timeout)
	return DelOperation{rch: ch, c: c}
}

func (c *Conn) CAS(key []byte, value []byte, timeout time.Duration) CASOperation {
	if timeout <= 0 {
		panic("CAS timeout <=0")
	}
	ch := c.send(protocol.OpCAS, key, value, timeout)
	return CASOperation{rch: ch, c: c}
}

func (c *Conn) SetNoDelay() {
	c.send(protocol.OpSetNoDelay, nil, nil, 0)
}

func (c *Conn) SetDynamicBuffering() {
	c.send(protocol.OpSetDynamicBuffering, nil, nil, 0)
}

func (c *Conn) SetBuffered() {
	c.send(protocol.OpSetBuffered, nil, nil, 0)
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	key, err := json.Marshal(chunkID)
	if err != nil {
		panic(err)
	}
	value := []byte(addr)
	r := c.sendAndReceive(protocol.OpTransfer, key, value, 500*time.Millisecond)
	return r.Err
}

//GetAccessInfo request DB access info
func (c *Conn) GetAccessInfo() ([]byte, error) {
	r := c.sendAndReceive(protocol.OpGetConf, nil, nil, 500*time.Millisecond)
	return r.Value, r.Err
}

//AddServerToGroup request to add this server to the server group
func (c *Conn) AddServerToGroup(addr string) error {
	key := []byte(addr)
	r := c.sendAndReceive(protocol.OpAddServerToGroup, key, nil, 500*time.Millisecond)
	return r.Err
}

func (c *Conn) Protect(chunkID int) error {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndReceive(protocol.OpProtect, key, nil, 500*time.Millisecond)
	if r.Err != nil {
		return r.Err
	}
	return nil
}

//GetChunkInfo request chunk info
func (c *Conn) GetChunkInfo(chunkID int) (size uint64, err error) {
	key := make([]byte, 4) //TODO static array
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
	r := c.sendAndReceive(protocol.OpGetChunkInfo, key, nil, 500*time.Millisecond)
	if r.Err != nil {
		return 0, r.Err
	}
	return binary.LittleEndian.Uint64(r.Value), nil
}
