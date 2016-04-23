package pmap

import "time"

/*
	newTime | newChecksum | mediumTime | mediumChecksum | oldTime | currentChecksum

*/

type syncChecksum struct {
	newChecksum                  uint64
	mediumChecksum               uint64
	currentChecksum              uint64
	newTime, mediumTime, oldTime time.Time
	interval                     time.Duration
}

func (s *syncChecksum) SetInterval(i time.Duration) {
	s.interval = i
}

func (s *syncChecksum) Checksum() uint64 {
	return s.currentChecksum
}

func (s *syncChecksum) Sub(el uint64, t time.Time) {
	s.Sum(-el, t)
}

func (s *syncChecksum) Sum(el uint64, t time.Time) {
	if t.After(s.newTime) {
		//Move forward the time
		s.oldTime = s.mediumTime
		s.mediumTime = s.newTime
		s.newTime = time.Unix(int64((time.Now().Unix()))+1, 0)
		s.currentChecksum = s.mediumChecksum
		s.mediumChecksum = s.newChecksum
	}
	if t.Before(s.newTime) {
		s.newChecksum += el
	}
	if t.Before(s.mediumTime) {
		s.mediumChecksum += el
	}
	if t.Before(s.oldTime) {
		s.currentChecksum += el
	}

}
