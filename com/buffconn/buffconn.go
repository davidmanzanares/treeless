package buffconn

import (
	"encoding/binary"
	"log"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"treeless/com/protocol"
)

//For maximum performance set this variable to the MSS(maximum segment size)
const bufferSize = 1450

//High values favours throughput (in non-sequential workloads), low values favours low Latency
const windowTimeDuration = time.Microsecond * 250

const fastModeEnableProbability = 1 / 500.0

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

//NewBufferedConn creates a new buffered tlproto connection that uses an existing TCP connection (conn).
//It returns a toWorldChannel, use it to send messages throught conn
//It recieves a fromWorld, inconming messages will be sent to this channel
//Close it by closing the writeChannel, the tcp connection and the fromWorld channel will be closed by the bufferedconn afterwards
//If a TCP connection error happens the readChannel will be closed, caller is responsible of closing toWorldChannel to free resources
func NewBufferedConn(conn *net.TCPConn, fromWorld chan<- protocol.Message) (toWorldChannel chan<- protocol.Message) {
	toWorld := make(chan protocol.Message, 1024)
	offset := new(int32)
	go bufferedWriter(conn, toWorld, offset)
	go bufferedReader(conn, fromWorld, offset)
	return toWorld
}

func tcpWrite(conn *net.TCPConn, buffer []byte) {
	for len(buffer) > 0 {
		n, err := conn.Write(buffer)
		if err != nil {
			conn.Close()
			log.Println(err)
		}
		buffer = buffer[n:]
	}
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
func bufferedWriter(conn *net.TCPConn, toWorld <-chan protocol.Message, offset *int32) {
	ticker := time.NewTicker(windowTimeDuration)
	var tickerChannel <-chan time.Time

	buffer := make([]byte, bufferSize)
	index := 0 //buffer write index

	sents := 0
	for {
		/*if rand.Float32() > 0.999 {
			fmt.Println(ticker)
		}*/
		select {
		case m, ok := <-toWorld:
			if !ok {
				//Channel closed, stop loop
				ticker.Stop()
				conn.Close()
				return
			}
			atomic.AddInt32(offset, 1)
			//Append message to buffer
			msgSize, tooLong := m.Marshal(buffer[index:])
			if tooLong {
				//Message too long for the buffer remaining space
				//Flush old data on buffer
				tcpWrite(conn, buffer[:index])
				index = 0
				if msgSize > bufferSize {
					//Too big message for the buffer (even if empty)
					//Send this message
					bigMsg := make([]byte, msgSize)
					m.Marshal(bigMsg)
					tcpWrite(conn, bigMsg)
				} else {
					//Fast path: add msg to the buffer
					m.Marshal(buffer)
					index += msgSize
				}
			} else if tickerChannel != nil {
				//Fast path
				index += msgSize
				sents++
			} else {
				//Slow path
				if fastModeEnableProbability > 0 && (atomic.LoadInt32(offset) > 1 || rand.Float32() < fastModeEnableProbability) {
					//Activate fast path
					tickerChannel = ticker.C
				}
				tcpWrite(conn, buffer[:msgSize])
			}
		case <-tickerChannel:
			//Flush buffer
			tcpWrite(conn, buffer[0:index])
			index = 0
			if sents < 2 {
				//Deactivate fast path
				tickerChannel = nil
				tcpWrite(conn, buffer[0:index])
				index = 0
				sents = 0
			}
			sents = sents / 4
		}
	}
}

//bufferedReader reads messages from conn and sends them to fromWorld
//Close the socket to close the reader, the reader will close the channel afterwards
//TCP errors will close the channel
//This function blocks, typical usage: "go bufferedReader(...)"
func bufferedReader(conn *net.TCPConn, fromWorld chan<- protocol.Message, offset *int32) error {
	//Ping-pong between buffers
	var slices [2][]byte
	slices[0] = make([]byte, bufferSize)
	slices[1] = make([]byte, bufferSize)
	slot := 0
	buffer := slices[slot]

	index := 0 //Write index
	for {
		if index < protocol.MinimumMessageSize {
			//Not enought bytes read to form a message, read more
			n, err := conn.Read(buffer[index:])
			if err != nil {
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
					close(fromWorld)
					return err
				}
				index = index + n
			}
			msg := protocol.Unmarshal(bigBuffer)
			fromWorld <- msg
			index = 0
			continue
		}
		if index < messageSize {
			//Not enought bytes read to form *this* message, read more
			n, err := conn.Read(buffer[index:])
			if err != nil {
				close(fromWorld)
				return err
			}
			index = index + n
			continue
		}

		atomic.AddInt32(offset, -1)
		msg := protocol.Unmarshal(buffer[:messageSize])
		fromWorld <- msg

		//Buffer ping-pong
		copy(slices[(slot+1)%2], buffer[messageSize:index])
		slot = (slot + 1) % 2
		index = index - messageSize
		buffer = slices[slot]
	}
}
