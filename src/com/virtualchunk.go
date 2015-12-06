package tlcom

import "time"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	ID      int
	Holders map[*VirtualServer]bool //Set of server holders
	//Rebalancer data
	timeToReview time.Time
	index        int
}
