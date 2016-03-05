package tlsg

import "fmt"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	ID      int
	Holders map[*VirtualServer]bool //Set of server holders TODO: array
}

func (c *VirtualChunk) String() {
	str := "Chunk" + fmt.Sprint(c.ID)
	str += "\nHolders:"
	for k := range c.Holders {
		str += " " + k.Phy
	}
}
