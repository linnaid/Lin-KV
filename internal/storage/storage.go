package storage

type LogEntry struct {
	Term    int
	Command []byte
	Index   int
}

type LogStorage interface {
	Append(entry *LogEntry) error
	Reply() ([]*LogEntry, error)
}