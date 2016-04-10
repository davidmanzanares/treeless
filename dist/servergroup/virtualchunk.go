package servergroup

import "fmt"

//VirtualChunk stores generical chunk info, including server holders
type VirtualChunk struct {
	id      int
	holders []*VirtualServer //Set of server holders TODO: array
}

func (c *VirtualChunk) String() {
	str := "Chunk" + fmt.Sprint(c.id)
	str += "\nHolders:"
	for _, h := range c.holders {
		str += " " + h.Phy
	}
}

func (c *VirtualChunk) hasHolder(s *VirtualServer) bool {
	for _, sin := range c.holders {
		if s == sin {
			return true
		}
	}
	return false
}

func (c *VirtualChunk) removeHolder(s *VirtualServer) {
	for i := 0; i < len(c.holders); i++ {
		if c.holders[i] == s {
			c.holders[i] = c.holders[len(c.holders)-1]
			c.holders = c.holders[:len(c.holders)-1]
			break
		}
	}
}

func (c *VirtualChunk) addHolder(s *VirtualServer) {
	c.holders = append(c.holders, s)
}
