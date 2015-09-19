package tlcom

import (
	"encoding/binary"
	"net"
	"time"
)

const bufferSize = 2048

const minimumMessageSize = 13
const windowTimeDuration = time.Microsecond * 1

//Operation represents a DB operation or result, the Message type
type Operation uint8

//These constants represents the different message types
const (
	OpGet Operation = iota
	OpPut
	OpDel
	OpSet
	OpGetResponse
	OpGetResponseError
	OpErr
	OpNil
)

//Message represents a DB message that can be sent and recieved using a network
type Message struct {
	Type       Operation
	ID         uint32
	Key, Value []byte
}

//Write serializes the message on the destination buffer
func (m *Message) write(dest []byte) int {
	size := len(m.Key) + len(m.Value) + 13

	binary.LittleEndian.PutUint32(dest[0:4], uint32(size))
	binary.LittleEndian.PutUint32(dest[4:8], m.ID)
	binary.LittleEndian.PutUint32(dest[8:12], uint32(len(m.Key)))
	dest[12] = byte(m.Type)
	copy(dest[13:], m.Key)
	copy(dest[13+len(m.Key):], m.Value)

	return size
}

//Read unserialize a message from a buffer
func read(src []byte) (m Message) {
	m.ID = binary.LittleEndian.Uint32(src[4:8])
	keySize := binary.LittleEndian.Uint32(src[8:12])
	m.Type = Operation(src[12])
	m.Key = src[13 : 13+keySize]
	m.Value = src[13+keySize:]
	return m
}

//TCPWriter will write to conn messages recieved by the channel
//
//This function implements buffering, and uses a time window:
//messages won't be written instantly, they will be written
//when the buffer gets filled or when a the timer wakes up the goroutine.
//
//Close the channel to stop the infinite listening loop.
//
//This function blocks, typical usage will be "go TCPWriter(...)""
func TCPWriter(conn *net.TCPConn, writeChannel chan Message) {
	timer := time.NewTimer(time.Second * 10000)
	timer.Stop()

	buffer := make([]byte, bufferSize)
	index := 0
	for {
		select {
		case <-timer.C:
			if index > 0 {
				conn.Write(buffer[0:index])
				index = 0
			}
			timer.Stop()
		case m, ok := <-writeChannel:
			if !ok {
				timer.Stop()
				return
			}
			//Bug big messages
			written := m.write(buffer[index:])
			index += written
			if index > 1350 {
				conn.Write(buffer[0:index])
				index = 0
				timer.Stop()
			} else {
				timer.Reset(windowTimeDuration)
			}
		}
	}
}

type ReaderCallback func(m Message)

//TCPReader calls callback each time a message is recieved by the conn TCP connection
//Close the socket to end the infinite listening loop
//This function blocks, typical usage: "go TCPReader(...)"
func TCPReader(conn net.Conn, callback ReaderCallback) error {
	var slices [2][]byte
	slices[0] = make([]byte, bufferSize)
	slices[1] = make([]byte, bufferSize)
	sli := 0
	buffer := slices[sli]

	index := 0
	for {
		if index < minimumMessageSize {
			n, err := conn.Read(buffer[index:])
			if err != nil {
				return err
			}
			index = index + n
			continue
		}
		messageSize := int(binary.LittleEndian.Uint32(buffer[0:4]))
		if index < messageSize {
			n, err := conn.Read(buffer[index:])
			if err != nil {
				return err
			}
			index = index + n
			continue
		}

		callback(read(buffer[:messageSize]))

		copy(slices[(sli+1)%2], buffer[messageSize:index])
		sli = (sli + 1) % 2
		index = index - messageSize
		buffer = slices[sli]
	}
}
