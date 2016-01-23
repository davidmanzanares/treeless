package tlsg

import "time"
import "fmt"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	ID      int
	Holders map[*VirtualServer]bool //Set of server holders
	//1024r data
	timeToReview time.Time
	index        int
}

func (c *VirtualChunk) String() {
	str := "Chunk" + fmt.Sprint(c.ID)
	str += "\nHolders:"
	for k, _ := range c.Holders {
		str += " " + k.Phy
	}
}
