package buffconn

import (
	"encoding/binary"
	"net"
	"sync"
	"time"
	"github.com/dv343/treeless/com/protocol"
)

//For maximum performance set this variable to the MSS(maximum segment size)
const writeBufferSize = 1450

const readBufferSize = 2048

//High values favours throughput (in non-sequential workloads), low values favours low Latency
const windowTimeDuration = time.Microsecond * 8 * 250

const fastModeEnableProbability = 1 / 500.0

type Conn struct {
	tcp                *net.TCPConn
	readBuffer         []byte
	readStart, readEnd int
	writeBuffer        []byte
	writeIndex         int
	buffering          bool
	writeMutex         sync.Mutex
	ch                 chan int
}

func New(conn *net.TCPConn) *Conn {
	c := new(Conn)
	c.tcp = conn
	c.readBuffer = make([]byte, readBufferSize)
	c.writeBuffer = make([]byte, writeBufferSize)
	c.ch = make(chan int, 1)
	go func(c *Conn) {
		var ticker *time.Ticker
		for {
			if ticker == nil {
				op, ok := <-c.ch
				if !ok {
					return
				} else if op == 1 {
					ticker = time.NewTicker(windowTimeDuration)
				}
			} else {
				select {
				case op, ok := <-c.ch:
					if !ok {
						ticker.Stop()
						ticker = nil
						return
					} else if op == 0 {
						ticker.Stop()
						ticker = nil
					}
				case <-ticker.C:
					c.writeMutex.Lock()
					tcpWrite(c.tcp, c.writeBuffer[:c.writeIndex])
					c.writeIndex = 0
					c.writeMutex.Unlock()
				}
			}
		}
	}(c)
	c.buffering = true
	c.ch <- 1
	return c
}

func (c *Conn) Read() (protocol.Message, error) {
	for {
		//Read at least the size of the message
		for c.readEnd-c.readStart < protocol.MinimumMessageSize {
			//Not enough bytes read to form a message, read more
			n, err := c.tcp.Read(c.readBuffer[c.readEnd:])
			if err != nil {
				c.tcp.Close()
				return protocol.Message{}, err
			}
			c.readEnd = c.readEnd + n
		}
		messageSize := int(binary.LittleEndian.Uint32(c.readBuffer[c.readStart:]))

		//Special treatment for big messages
		if messageSize > readBufferSize-c.readStart {
			//Big message
			bigBuffer := make([]byte, messageSize)
			//Copy read part of the message
			copy(bigBuffer, c.readBuffer[c.readStart:c.readEnd])
			//Read until the message is complete
			index := c.readEnd - c.readStart
			for index < messageSize {
				n, err := c.tcp.Read(bigBuffer[index:])
				if err != nil {
					c.tcp.Close()
					return protocol.Message{}, err
				}
				index += n
			}
			msg := protocol.Unmarshal(bigBuffer)
			c.readStart = 0
			c.readEnd = 0
			return msg, nil
		}

		//Read the remaining bytes
		for c.readEnd-c.readStart < messageSize {
			//Not enough bytes read to form this message, read more
			n, err := c.tcp.Read(c.readBuffer[c.readEnd:])
			if err != nil {
				c.tcp.Close()
				return protocol.Message{}, err
			}
			c.readEnd += n
		}

		//Maintain the buffer before returning
		if c.readStart > 1400 {
			copy(c.readBuffer, c.readBuffer[c.readStart:c.readEnd])
			c.readEnd = c.readEnd - c.readStart
			c.readStart = 0
		}
		c.readStart = c.readStart + messageSize
		return protocol.Unmarshal(c.readBuffer[c.readStart-messageSize : c.readStart]), nil
	}
}

func tcpWrite(conn *net.TCPConn, buffer []byte) error {
	for len(buffer) > 0 {
		n, err := conn.Write(buffer)
		if err != nil {
			conn.Close()
			return err
		}
		buffer = buffer[n:]
	}
	return nil
}

func (c *Conn) Write(m protocol.Message) error {
	c.writeMutex.Lock()
	if m.Type == protocol.OpSetBuffered {
		c.buffering = true
		c.ch <- 1
	} else if m.Type == protocol.OpSetNoDelay {
		c.buffering = false
		c.ch <- 0
	}
	//Append message to buffer
	msgSize, tooLong := m.Marshal(c.writeBuffer[c.writeIndex:])
	if tooLong {
		//Message too long for the buffer remaining space
		//Flush old data on buffer
		err := tcpWrite(c.tcp, c.writeBuffer[:c.writeIndex])
		if err != nil {
			c.writeMutex.Unlock()
			return err
		}
		c.writeIndex = 0
		if msgSize > writeBufferSize {
			//Too big message for the buffer (even if empty)
			//Send this message
			bigMsg := make([]byte, msgSize)
			m.Marshal(bigMsg)
			err := tcpWrite(c.tcp, bigMsg)
			c.writeMutex.Unlock()
			return err
		} else {
			//Add msg to the buffer
			m.Marshal(c.writeBuffer)
		}
	}
	if !c.buffering {
		c.writeIndex += msgSize
		err := tcpWrite(c.tcp, c.writeBuffer[:c.writeIndex])
		c.writeIndex = 0
		c.writeMutex.Unlock()
		return err
	} else {
		c.writeIndex += msgSize
		c.writeMutex.Unlock()
		return nil
	}
}

func (c *Conn) Close() {
	close(c.ch)
	c.tcp.Close()
}
