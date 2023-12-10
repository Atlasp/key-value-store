package transcationlog

import (
	"bufio"
	"fmt"
	"os"
)

const FORMAT = "%d\t%d\t%s\t%s\n"

type FileTransactionLog struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func NewFileTransactionLog(filename string) (*FileTransactionLog, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLog{
		file: file,
	}, nil
}

func (l *FileTransactionLog) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLog) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLog) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLog) Run() {
	events := make(chan Event, 16)
	l.events = events
	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(l.file, FORMAT, l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLog) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error)

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, FORMAT,
				&e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
		}

		isStartingSequence := (l.lastSequence == 0) && (e.Sequence == 0)

		if l.lastSequence >= e.Sequence && !isStartingSequence {
			outError <- fmt.Errorf("transaction numbers out of sequence")
			return
		}

		l.lastSequence = e.Sequence

		outEvent <- e
	}()

	return outEvent, outError
}

func (l *FileTransactionLog) Close() error {
	return l.file.Close()
}
