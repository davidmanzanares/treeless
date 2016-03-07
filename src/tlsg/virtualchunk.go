package tlsg

import "fmt"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	id      int
	holders map[*VirtualServer]bool //Set of server holders TODO: array
}

func (c *VirtualChunk) String() {
	str := "Chunk" + fmt.Sprint(c.id)
	str += "\nHolders:"
	for k := range c.holders {
		str += " " + k.Phy
	}
}
