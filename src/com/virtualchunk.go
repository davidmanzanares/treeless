package tlcom

import "time"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	id           int
	holders      map[*VirtualServer]bool //Each chunk has a list of holders, servers that has this chunk
	timeToReview time.Time
	index        int
}
