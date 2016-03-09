package tlutils

import (
	"fmt"
	"sync"
)

type Progress struct {
	reason           string
	index            int
	total            int
	lastPrintedIndex int
	m                sync.Mutex
}

func NewProgress(reason string, total int) *Progress {
	p := &Progress{reason: reason, total: total}
	p.print(0)
	return p
}

func (p *Progress) Inc() {
	p.m.Lock()
	p.index++
	if p.index-p.lastPrintedIndex > p.total/1000 {
		p.lastPrintedIndex = p.index
		index := p.index
		p.m.Unlock()
		p.print(index)
		return
	}
	p.m.Unlock()
}

func (p *Progress) print(index int) {
	if index == p.total {
		fmt.Printf("\r%s %.2f%%\t\t\n", p.reason, 100.0)
	} else {
		fmt.Printf("\r%s %.2f%%\t\t", p.reason, 100.0*float64(index)/float64(p.total))
	}
}
