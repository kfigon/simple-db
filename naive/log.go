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
	// todo: implement log entry and serialization
	return nil
}

func DeserializeLogEntry(d []byte) (LogEntry, error) {
	// todo
	return LogEntry{},nil
}

func NewLog(p *GenericPage) *Log {
	return &Log{
		p: p,
	}
}

func (l *Log) Append(s *Storage, entry LogEntry) LSN {
	_, err := l.p.Add(entry.Serialize())
	if errors.Is(err, errNoSpace) {
		_, newPage := s.allocatePage(LogPageType, "wal_log")
		l.p = newPage
		return -1
	}

	l.lastLsn++
	return l.lastLsn
}

func (l *Log) Iterator(s *Storage) iter.Seq[LogEntry] {
	return func(yield func(LogEntry) bool) {
		for i := range s.iter().NewEntityIterator(LogPageType, "wal_log") {
			if !yield(LogEntry{}) {
				return 
			}
	}
}
