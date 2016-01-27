package tlTCP

import (
	"encoding/binary"
	"net"
	"time"
)

const bufferSize = 1800
const bufferSizeTrigger = 1350
const minimumMessageSize = 13
const windowTimeDuration = time.Microsecond * 1

//Operation represents a DB operation or result, the Message type
type Operation uint8

//These constants represents the different message types
const (
	OpGet Operation = iota
	OpSet
	OpDel
	OpSetOK
	OpGetResponse
	OpGetConf
	OpGetConfResponse
	OpAddServerToGroup
	OpAddServerToGroupACK
	OpGetChunkInfo
	OpGetChunkInfoResponse
	OpTransfer
	OpTransferCompleted
	OpErr
)

//Message represents a DB message that can be sent and recieved using a network
type Message struct {
	Type       Operation
	ID         uint32
	Key, Value []byte
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

//Read unserialize a message from a buffer
func read(src []byte) (m Message) {
	m.ID = binary.LittleEndian.Uint32(src[4:8])
	keySize := binary.LittleEndian.Uint32(src[8:12])
	m.Type = Operation(src[12])
	m.Key = src[13 : 13+keySize]
	m.Value = src[13+keySize:]
	return m
}

//Writer will write to conn messages recieved by the channel
//
//This function implements buffering, and uses a time window:
//messages won't be written instantly, they will be written
//when the buffer gets filled or when a the timer wakes up the goroutine.
//
//Close the channel to stop the infinite listening loop.
//
//This function blocks, typical usage will be "go Writer(...)""
func Writer(conn *net.TCPConn, msgChannel chan Message) {
	timer := time.NewTimer(time.Hour)
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
		case m, ok := <-msgChannel:
			if !ok {
				//Channel closed, stop loop
				timer.Stop()
				return
			}
			//Append message to buffer
			msgSize, tooLong := m.write(buffer[index:bufferSize])
			if tooLong {
				//Big message
				//Send previous buffer
				if index > 0 {
					conn.Write(buffer[:index])
					index = 0
				}
				timer.Stop()
				//Send this message
				bigMsg := make([]byte, msgSize)
				m.write(bigMsg)
				conn.Write(bigMsg)
				continue
			}
			index += msgSize
			if index > bufferSizeTrigger {
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

//Reader calls callback each time a message is recieved by the conn TCP connection
//The message passed to the callback wont live after the callback returns, it should copy the message
//Close the socket to end the infinite listening loop
//This function blocks, typical usage: "go Reader(...)"
func Reader(conn net.Conn, callback ReaderCallback) error {
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
					return err
				}
				index = index + n
			}
			callback(read(bigBuffer))
			index = 0
			continue
		}
		if index < messageSize {
			//Not enought bytes read to form *this* message, read more
			n, err := conn.Read(buffer[index:])
			if err != nil {
				return err
			}
			index = index + n
			continue
		}

		callback(read(buffer[:messageSize]))

		//Buffer ping-pong
		//TODO opt: dont need to copy everytime, be smart
		copy(slices[(slot+1)%2], buffer[messageSize:index])
		slot = (slot + 1) % 2
		index = index - messageSize
		buffer = slices[slot]
	}
}
