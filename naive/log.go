package naive

import (
	"errors"
	"iter"
)

// sequential number to identify log records
type LSN int32
type Log struct {
	p *GenericPage
	lastLsn LSN
}

type LogEntry struct{}
func (le LogEntry) Serialize() []byte {
	debugAssert(false, "implement log entry and serialization")
	return nil
}

func NewLog(p *GenericPage) *Log {
	return &Log{
		p: p,
	}
}

func (l *Log) Append(s *Storage, entry LogEntry) LSN {
	_, err := l.p.Add(entry.Serialize())
	if errors.Is(err, errNoSpace) {
		debugAssert(false, "todo: implement overflow log pages")
		_, newPage := s.allocatePage(LogPageType, "wal_log")
		l.p = newPage
		return -1
	}

	l.lastLsn++
	return l.lastLsn
}

func (l *Log) Iterator() iter.Seq[LogEntry] {
	return func(yield func(LogEntry) bool) {
		debugAssert(false, "todo: implement log entry iterator")

		if !yield(LogEntry{}) {
			return 
		}
	}
}
