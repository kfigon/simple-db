package naive

import (
	"iter"
)

// sequential number to identify log records
type LSN int32
type Log struct {
	p       *GenericPage
	lastLsn LSN
}

type LogEntry struct {
	lsn LSN
	//... page/slot []change
	// []table-columns-change
}

func (le LogEntry) Serialize() []byte {
	// todo: implement log entry and serialization
	return nil
}

func DeserializeLogEntry(d []byte) (LogEntry, error) {
	// todo
	return LogEntry{}, nil
}

func NewLog(p *GenericPage) *Log {
	return &Log{
		p: p,
	}
}

func (l *Log) Append(entry LogEntry) LSN {
	// todo
	l.lastLsn++
	return l.lastLsn
}

func (l *Log) Iterator() iter.Seq[LogEntry] {
	return func(yield func(LogEntry) bool) {
		// for range s.iter().NewEntityIterator(LogPageType, "wal_log") {
		// 	if !yield(LogEntry{}) {
		// 		return
		// 	}
		// }
	}
}
