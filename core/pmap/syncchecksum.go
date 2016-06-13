package pmap

import "time"

type syncChecksum struct {
	newChecksum, mediumChecksum, oldChecksum uint64
	newTime, mediumTime, oldTime             time.Time
}

func (s *syncChecksum) checksum() uint64 {
	s.sum(0, time.Now())
	return s.oldChecksum
}

func (s *syncChecksum) sub(el uint64, t time.Time) {
	s.sum(-el, t)
}

func (s *syncChecksum) sum(el uint64, t time.Time) {
	if t.After(s.newTime) {
		//Move forward the time
		s.oldTime = s.mediumTime
		s.mediumTime = s.newTime
		s.newTime = time.Unix(t.Unix()+1, 0)
		s.oldChecksum = s.mediumChecksum
		s.mediumChecksum = s.newChecksum
	}
	s.newChecksum += el
	if t.Before(s.mediumTime) {
		s.mediumChecksum += el
	}
	if t.Before(s.oldTime) {
		s.oldChecksum += el
	}
}
