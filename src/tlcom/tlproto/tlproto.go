package tlproto

import (
	"encoding/binary"
	"log"
	"net"
	"time"
)

//For maximum performance set this variable to the MSS(maximum segment size)
const bufferSize = 1450

//High values favours throughput, low values favours low Latency
const windowTimeDuration = time.Microsecond * 100
const windowFastModeEnable = time.Microsecond * 10

/*
	TCP treeless protocol

	Each message is composed by:
		0:4 bytes:						message size
		4:8 bytes:						message ID
		8:12 bytes:						message key len
		12 byte:						operation type
		13:13+key len bytes:			key
		13+key len:message size bytes:	value

*/

const minimumMessageSize = 13

const fastModeEnable = true

//Operation represents a DB operation or result, the Message type
type Operation uint8

//These constants represents the different message types
const (
	OpNil Operation = iota
	OpGet
	OpSet
	OpDel
	OpSetOK
	OpDelOK
	OpGetResponse
	OpGetConf
	OpGetConfResponse
	OpAddServerToGroup
	OpAddServerToGroupACK
	OpGetChunkInfo
	OpGetChunkInfoResponse
	OpTransfer
	OpTransferCompleted
	OpProtect
	OpProtectOK
	OpErr
)

//Message represents a DB message that can be sent and recieved using a network
type Message struct {
	Type            Operation
	ID              uint32
	Key, Value      []byte
	ResponseChannel chan Message //Only used for inconming messages, buffered reader will set it to the connection write channel
}

//NewBufferedConn creates a new buffered tlproto connection that uses an existing TCP connection (conn).
//It returns a writeChannel, use it to send messages throught conn
//It recieves a readChannel, inconming messages will be sent to this channel
//Close it by closing conn and writeChannel
func NewBufferedConn(conn *net.TCPConn, fromWorld chan<- Message) (toWorldChannel chan<- Message) {
	toWorld := make(chan Message, 1024)
	go bufferedWriter(conn, toWorld)
	go bufferedReader(conn, fromWorld, toWorld)
	return toWorld
}

//Write serializes the message on the destination buffer
func (m *Message) write(dest []byte) (msgSize int, tooLong bool) {

	size := len(m.Key) + len(m.Value) + 13

	if size > len(dest) {
		return size, true
	}

	binary.LittleEndian.PutUint32(dest[0:4], uint32(size))
	binary.LittleEndian.PutUint32(dest[4:8], m.ID)
	binary.LittleEndian.PutUint32(dest[8:12], uint32(len(m.Key)))
	dest[12] = byte(m.Type)
	copy(dest[13:], m.Key)
	copy(dest[13+len(m.Key):], m.Value)
	return size, false
}

//Read unserializes a message from a buffer
func read(src []byte) (m Message) {
	m.ID = binary.LittleEndian.Uint32(src[4:8])
	keySize := binary.LittleEndian.Uint32(src[8:12])
	m.Type = Operation(src[12])
	array := make([]byte, len(src[13:]))
	copy(array, src[13:])
	m.Key = array[:keySize]
	m.Value = array[keySize:]
	return m
}

func tcpWrite(conn *net.TCPConn, buffer []byte) error {
	for len(buffer) > 0 {
		n, err := conn.Write(buffer)
		if err != nil {
			conn.Close()
			log.Println(err)
			return err
		}
		buffer = buffer[n:]
	}
	return nil
}

//bufferedWriter will write to conn messages recieved by the channel
//
//This function implements buffering, and uses a time window:
//messages won't be written instantly, they will be written
//when the buffer gets filled or when a the timer wakes up the goroutine.
//
//Close the channel to stop the infinite listening loop.
//
//This function blocks, typical usage will be "go bufferedWriter(...)""
func bufferedWriter(conn *net.TCPConn, toWorld <-chan Message) {
	var ticker *time.Ticker
	dirty := false
	buffer := make([]byte, bufferSize)
	index := 0
	lastTime := time.Now()
	sents := 0
	for {
		/*if rand.Float32() > 0.99 {
			fmt.Println(ticker)
		}*/
		if ticker == nil {
			//Slow path
			m, ok := <-toWorld
			if !ok {
				//Channel closed, stop loop
				conn.Close()
				return
			}
			//Append message to buffer
			msgSize, tooLong := m.write(buffer[index:])
			sents++
			if !tooLong {
				err := tcpWrite(conn, buffer[0:msgSize])
				if err != nil {
					continue
				}
				if fastModeEnable && time.Now().Sub(lastTime) < windowFastModeEnable {
					//Activate fast mode
					ticker = time.NewTicker(windowTimeDuration)
				} else {
					lastTime = time.Now()
				}
			} else {
				bigMsg := make([]byte, msgSize)
				m.write(bigMsg)
				err := tcpWrite(conn, bigMsg)
				if err != nil {
					continue
				}
			}
		} else {
			select {
			case <-ticker.C:
				if index > 0 && !dirty {
					err := tcpWrite(conn, buffer[0:index])
					if err != nil {
						continue
					}
					index = 0
				}
				dirty = false
				fast := sents > 1
				if !fast && index > 0 {
					//flush now
					err := tcpWrite(conn, buffer[0:index])
					if err != nil {
						continue
					}
					index = 0
				}
				if !fast {
					ticker.Stop()
					ticker = nil
				}
				sents = sents / 8
			case m, ok := <-toWorld:
				if !ok {
					//Channel closed, stop loop
					ticker.Stop()
					conn.Close()
					return
				}
				//Append message to buffer
				msgSize, tooLong := m.write(buffer[index:])
				sents++
				if tooLong {
					//Message too long for the buffer remaining space
					if index > 0 {
						//Send buffer
						err := tcpWrite(conn, buffer[:index])
						if err != nil {
							continue
						}
						index = 0
					}
					if msgSize > bufferSize {
						//Too big message for the buffer (even if empty)
						//Send this message
						bigMsg := make([]byte, msgSize)
						m.write(bigMsg)
						err := tcpWrite(conn, bigMsg)
						if err != nil {
							continue
						}
					} else {
						//Add msg to the buffer
						m.write(buffer)
						index += msgSize
						dirty = true
					}
				} else {
					//Fast path
					index += msgSize
					dirty = true
				}
			}
		}
	}
}

//bufferedReader reads messages from conn and sends them to readChannel
//Close the socket to end the infinite listening loop
//This function blocks, typical usage: "go bufferedReader(...)"
func bufferedReader(conn *net.TCPConn, fromWorld chan<- Message, toWorld chan Message) error {
	//Ping-pong between buffers
	var slices [2][]byte
	slices[0] = make([]byte, bufferSize)
	slices[1] = make([]byte, bufferSize)
	slot := 0
	buffer := slices[slot]

	index := 0 //Write index
	for {
		if index < minimumMessageSize {
			//Not enought bytes read to form a message, read more
			n, err := conn.Read(buffer[index:])
			if err != nil {
				//conn.Close()
				close(fromWorld)
				return err
			}
			index = index + n
			continue
		}
		messageSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
		if messageSize > bufferSize {
			//Big message
			bigBuffer := make([]byte, messageSize)
			//Copy read part of the message
			copy(bigBuffer, buffer[:index])
			//Read until the message is complete
			for index < messageSize {
				n, err := conn.Read(bigBuffer[index:])
				if err != nil {
					//conn.Close()
					close(fromWorld)
					return err
				}
				index = index + n
			}
			msg := read(bigBuffer)
			msg.ResponseChannel = toWorld
			fromWorld <- msg
			index = 0
			continue
		}
		if index < messageSize {
			//Not enought bytes read to form *this* message, read more
			n, err := conn.Read(buffer[index:])
			if err != nil {
				//conn.Close()
				close(fromWorld)
				return err
			}
			index = index + n
			continue
		}

		msg := read(buffer[:messageSize])
		msg.ResponseChannel = toWorld
		fromWorld <- msg

		//Buffer ping-pong
		copy(slices[(slot+1)%2], buffer[messageSize:index])
		slot = (slot + 1) % 2
		index = index - messageSize
		buffer = slices[slot]
	}
}
