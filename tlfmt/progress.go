package tlfmt

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
	p := &Progress{reason: reason, total: total - 1}
	p.print(0)
	return p
}

func (p *Progress) Inc() {
	p.m.Lock()
	p.index++
	if p.index-p.lastPrintedIndex > p.total/100 || p.index == p.total {
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
		fmt.Printf("\r%s %.0f%%\t\t\n", p.reason, 100.0)
	} else {
		fmt.Printf("\r%s %.0f%%\t\t", p.reason, 100.0*float64(index)/float64(p.total))
	}
}
