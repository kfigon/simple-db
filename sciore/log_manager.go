package sciore

// log sequence number, ID of the log entries
type LSN int

type LogManager struct {
	// runnig page with recent updates. Will be dropped to disk
	// when forced or when full. This is the last block
	// of the log file
	p *Page
}

func NewLogManager() *LogManager {
	return &LogManager{
		p: NewPage(),
	}
}

func (l *LogManager) append(data []byte) LSN {
	return 0
}

// noop
func (l *LogManager) flush(s *Storage) {}
