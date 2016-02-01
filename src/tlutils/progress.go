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
	sync.Mutex
}

//TODO multithread
func NewProgress(reason string, total int) *Progress {
	p := &Progress{reason: reason, total: total}
	p.print()
	return p
}

func (p *Progress) Set(index int) {
	p.Lock()
	p.index = index
	if p.index-p.lastPrintedIndex > p.total/1000 {
		p.print()
	}
	p.Unlock()
}

func (p *Progress) print() {
	p.lastPrintedIndex = p.index
	if p.index == p.total-1 {
		fmt.Printf("\r%s asdasd%.2f%%\t\t\n", p.reason, 100.0)
	} else {
		fmt.Printf("\r%s %.2f%%\t\t", p.reason, 100.0*float64(p.index)/float64(p.total))
	}
}
