package treeless

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

//Stores a DB operation result
type result struct {
	value []byte
	err   error
}

//Connection is a DB connection
type Connection struct {
	conn         net.Conn
	writeChannel chan tlcom.Message
	waitLock     sync.Mutex
	waits        map[uint32](chan result)
	tid          uint32
	chanPool     sync.Pool
	closed       int32
}

type serverID int

type virtualChunk struct {
	id           int        //Chunk ID
	allServers   []serverID //Ordered (by hash weight) list with the servers IDs
	knownServers []serverID //Ordered (by hash weight) list with the servers IDs, only connected servers are included
}

const (
	GetPolicyConnectedServers = 0
	GetPolicyOneServer        = 1
)

//Access provides an access to a DB server group
type Access struct {
	vChunks   []*virtualChunk
	conns     []*Connection //Maps each serverID with a connection
	GetPolicy int
}

func CreateAccess(ac *AccessConf) {
	//Create virtual chunks
	//Calculate server ranking for each chunk
	//Establish all server connections
	//Calculate adjusted server ranking (connected only)
}
func (a *Access) Get(key []byte) ([]byte, error) {
	//Calc hash
	//get vChunk
	//for each needed read
	//	get conn
	//	r[i]=conn.get
	//for each result
	//	if r[i]!=r[i+1]{
	//		return nil, error
	//return value, ok
	return nil, nil
}

//ToDo: use external Connection struct, and let it decide how many TCP connection must be used simultaneously

const bufferSize = 2048
const writeTimeWindow = 1000

func (c *Connection) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}
func (c *Connection) Close() {
	atomic.StoreInt32(&c.closed, 1)
	close(c.writeChannel)
	c.conn.Close()
}

func listenToResponses(c *Connection) {
	f := func(m tlcom.Message) {
		c.waitLock.Lock()
		ch := c.waits[m.ID]
		c.waitLock.Unlock()
		switch m.Type {
		case tlcom.OpGetResponse:
			rval := make([]byte, len(m.Value))
			copy(rval, m.Value)
			ch <- result{rval, nil}
		case tlcom.OpGetResponseError:
			ch <- result{nil, errors.New(string(m.Value))}
		}
	}
	err := tlcom.TCPReader(c.conn, f)
	if !c.isClosed() {
		panic(err)
	}
}

//Get the value of  key
func (c *Connection) Get(key []byte) ([]byte, error) {
	var mess tlcom.Message

	ch := c.chanPool.Get().(chan result)
	c.waitLock.Lock()
	mytid := c.tid
	c.waits[c.tid] = ch
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlcom.OpGet
	mess.Key = key
	mess.ID = mytid

	c.writeChannel <- mess
	r := <-ch
	c.chanPool.Put(ch)
	c.waitLock.Lock()
	delete(c.waits, mytid)
	c.waitLock.Unlock()
	return r.value, r.err
}

//Put a new key/value pair
func (c *Connection) Put(key, value []byte) error {
	var mess tlcom.Message

	c.waitLock.Lock()
	mytid := c.tid
	c.tid++
	c.waitLock.Unlock()

	mess.Type = tlcom.OpPut
	mess.ID = mytid
	mess.Key = key
	mess.Value = value

	//fmt.Println("sending put", key, value, len(string(key)), len(key))
	c.writeChannel <- mess
	return nil
}

//CreateConnection returns a new DB connection
func CreateConnection(IP string) *Connection {
	var c Connection
	c.chanPool.New = func() interface{} {
		return make(chan result)
	}

	c.waits = make(map[uint32](chan result))
	tcpconn, err := net.DialTCP("tcp", nil, &net.TCPAddr{IP: net.ParseIP(IP), Port: 9876})
	if err != nil {
		panic(err)
	}
	writeCh := make(chan tlcom.Message, 128)
	c.conn = tcpconn
	c.writeChannel = writeCh

	go tlcom.TCPWriter(tcpconn, writeCh)
	go listenToResponses(&c)

	return &c
}
