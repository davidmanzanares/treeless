package com

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"treeless/com/buffconn"
	"treeless/com/protocol"
)

//brokerTick controls the time between polling for timeout'ed respond messages
var brokerTick = time.Millisecond * 5

//dialTimeout controls the TCP dial timeout
var dialTimeout = 3 * time.Second

//result stores a DB operation result returned by a server
type result struct {
	Value []byte
	Err   error
}

type brokerMsg struct {
	mess    protocol.Message
	timeout time.Duration
	rch     chan result
}

//Conn provides an interface to a possible buffered DB TCP client connection
type Conn struct {
	tcpConn                  *net.TCPConn
	brokerSendChannel        chan brokerMsg
	brokerReceiveChannelPool sync.Pool
}

//CreateConnection returns a new DB connection
func CreateConnection(addr string, onClose func()) (*Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	tcpconn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	var c Conn
	c.tcpConn = tcpconn.(*net.TCPConn)
	//c.conn.SetNoDelay(false)

	c.brokerSendChannel = make(chan brokerMsg, 1024)
	c.brokerReceiveChannelPool = sync.Pool{New: func() interface{} {
		return make(chan result, 1)
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
	bconn := buffconn.New(c.tcpConn)
	waits := make(map[uint32]chan<- result)
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
						w <- result{nil, errors.New("Timeout" + fmt.Sprint("Local", c.tcpConn.LocalAddr(), "Remote", c.tcpConn.RemoteAddr()))}
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
			msg, ok := <-c.brokerSendChannel
			mutex.Lock()
			if !ok {
				pq.forall(func(tm *timeoutMsg) {
					w, ok := waits[tm.tid]
					if ok {
						if w == nil {
							panic("w rch == nil")
						}
						delete(waits, tm.tid)
						w <- result{nil, errors.New("Connection closed => fast timeout" + fmt.Sprint("Local", c.tcpConn.LocalAddr(), "Remote", c.tcpConn.RemoteAddr()))}
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
				ch <- result{m.Value, nil}
			case protocol.OpOK:
				ch <- result{nil, nil}
			case protocol.OpErr:
				ch <- result{nil, errors.New("Response error: " + string(m.Value))}
			default:
				ch <- result{nil, errors.New("Invalid response operation code: " + fmt.Sprint(m.Type))}
			}
		}
	}()
}

//Close this connection
func (c *Conn) Close() {
	close(c.brokerSendChannel)
}

func (c *Conn) send(opType protocol.Operation, key, value []byte,
	timeout time.Duration) chan result {
	if timeout == 0 {
		//Send-only
		var m brokerMsg
		m.mess.Type = opType
		m.mess.Key = key
		m.mess.Value = value
		c.brokerSendChannel <- m
		return nil
	}
	//Send and recieve
	var m brokerMsg
	m.mess.Type = opType
	m.mess.Key = key
	m.mess.Value = value
	m.timeout = timeout
	m.rch = c.brokerReceiveChannelPool.Get().(chan result)
	c.brokerSendChannel <- m
	return m.rch
}

func (c *Conn) sendAndReceive(opType protocol.Operation, key, value []byte,
	timeout time.Duration) result {
	rch := c.send(opType, key, value, timeout)
	if rch == nil {
		return result{}
	}
	return <-rch
}

type GetOperation struct {
	rch chan result
	c   *Conn
}

type SetOperation struct {
	rch chan result
	c   *Conn
}

type DelOperation struct {
	rch chan result
	c   *Conn
}

type CASOperation struct {
	rch chan result
	c   *Conn
}

func (g *GetOperation) Wait() result {
	if g.rch == nil {
		return result{nil, errors.New("Already returned")}
	}
	r := <-g.rch
	g.c.brokerReceiveChannelPool.Put(g.rch)
	g.rch = nil
	return r
}

func (g *SetOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.brokerReceiveChannelPool.Put(g.rch)
	g.rch = nil
	return r.Err
}

func (g *DelOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.brokerReceiveChannelPool.Put(g.rch)
	g.rch = nil
	return r.Err
}

func (g *CASOperation) Wait() error {
	if g.rch == nil {
		return errors.New("Already returned")
	}
	r := <-g.rch
	g.c.brokerReceiveChannelPool.Put(g.rch)
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
		op = protocol.OpAsyncSet
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

func (c *Conn) SetBuffered() {
	c.send(protocol.OpSetBuffered, nil, nil, 0)
}

//Transfer a chunk
func (c *Conn) Transfer(addr string, chunkID int) error {
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, uint32(chunkID))
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

/*
	UDP
*/
//UDPRequest sends an UDP request to the specified address with a timeout
func UDPRequest(addr string, timeout time.Duration) (response *protocol.AmAlive, err error) {
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	destAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(timeout))
	conn.WriteTo([]byte("ping"), destAddr)
	for {
		message := make([]byte, protocol.MaxHeartbeatSize)
		n, readAddr, err := conn.ReadFromUDP(message)
		if err != nil {
			return nil, err
		} else if readAddr.IP.Equal(destAddr.IP) {
			aa, err := protocol.AmAliveUnMarshal(message[:n])
			if err != nil {
				log.Println(err)
			}
			return aa, err

		}
	}
}
