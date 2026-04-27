package sciore

import "iter"

// log sequence number, ID of the log entries
type LSN int

type LogManager struct {
	// runnig page with recent updates. Will be dropped to disk
	// when forced or when full. This is the last block
	// of the log file
	p *Page
	s *Storage
}

func NewLogManager(s *Storage) *LogManager {
	return &LogManager{
		p: NewPage(),
		s: s,
	}
}

func (l *LogManager) append(data []byte) LSN {
	return 0
}

// noop
func (l *LogManager) flush(s *Storage) {}

type LogEntry []byte

// returns reverse order, as that's what recovery manager wants
func (l *LogManager) iter() iter.Seq[LogEntry] {
	return func(yield func(LogEntry) bool) {
		// todo
		if !yield(nil) {
			return
		}
	}
}
