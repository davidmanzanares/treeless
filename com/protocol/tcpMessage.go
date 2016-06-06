package protocol

import "encoding/binary"

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

//Operation represents a DB operation or result, the Message type
type Operation uint8

//These constants represents the different message types
const (
	//Primitives
	OpGet Operation = iota + 1
	OpSet
	OpSetAsync
	OpDel
	OpCAS
)
const (
	//Advanced ops
	OpAddServerToGroup Operation = iota + 100
	OpGetConf
	OpGetChunkInfo
	OpProtect
	OpTransfer
	OpAmAliveRequest
	OpSetDynamicBuffering
	OpSetBuffered
	OpSetNoDelay
)
const (
	//Responses
	OpOK Operation = iota + 200
	OpErr
	OpResponse
	)

//Message stores a DB message that can be sent and recieved using a network connection
type Message struct {
	Type       Operation
	ID         uint32
	Key, Value []byte
}

//MinimumMessageSize is the minimum size of every Message
const MinimumMessageSize = 13

//Marshal serializes the message on the destination buffer if the destination buffer has enought size
//If it doesn't it returns the message size and "true"
func (m *Message) Marshal(dest []byte) (msgSize int, tooLong bool) {

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

//Unmarshal unserializes a message from a buffer
//Returned message key and value are copied (src can be reused after calling this)
func Unmarshal(src []byte) (m Message) {
	m.ID = binary.LittleEndian.Uint32(src[4:8])
	keySize := binary.LittleEndian.Uint32(src[8:12])
	m.Type = Operation(src[12])
	array := make([]byte, len(src[13:]))
	copy(array, src[13:])
	m.Key = array[:keySize]
	m.Value = array[keySize:]
	return m
}
