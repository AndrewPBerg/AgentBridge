package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

var ErrJournalPoisoned = errors.New("journal is poisoned; restart the daemon before retrying")

type appendFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type Journal struct {
	mu       sync.Mutex
	file     appendFile
	path     string
	poisoned bool
}

func Open(path string) (*Journal, []protocol.Event, error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, nil, fmt.Errorf("create journal directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, nil, fmt.Errorf("secure journal directory: %w", err)
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, nil, fmt.Errorf("inspect journal: %w", statErr)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open journal: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("secure journal: %w", err)
	}
	events, validBytes, err := readEvents(file)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if err := file.Truncate(validBytes); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("truncate partial journal tail: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("sync repaired journal: %w", err)
	}
	if created {
		if err := syncDirectory(directory); err != nil {
			file.Close()
			return nil, nil, err
		}
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("seek journal end: %w", err)
	}
	return &Journal{file: file, path: path}, events, nil
}

func readEvents(file *os.File) ([]protocol.Event, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek journal: %w", err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, 0, fmt.Errorf("read journal: %w", err)
	}
	validLength := len(content)
	if len(content) > 0 && content[len(content)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(content, '\n')
		validLength = lastNewline + 1
	}
	var events []protocol.Event
	for index, line := range bytes.Split(content[:validLength], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event protocol.Event
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, 0, fmt.Errorf("decode journal line %d: %w", index+1, err)
		}
		if event.Version != protocol.Version {
			return nil, 0, fmt.Errorf("journal line %d has unsupported version %d", index+1, event.Version)
		}
		events = append(events, event)
	}
	return events, int64(validLength), nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}

func (j *Journal) Append(event protocol.Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return errors.New("journal is closed")
	}
	if j.poisoned {
		return ErrJournalPoisoned
	}
	line, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode journal event: %w", err)
	}
	line = append(line, '\n')
	written, err := j.file.Write(line)
	if err != nil || written != len(line) {
		j.poisoned = true
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("append journal event (%d/%d bytes): %w: %w", written, len(line), err, ErrJournalPoisoned)
	}
	if err := j.file.Sync(); err != nil {
		// The event may or may not be durable. Never allow another append in this
		// process: retrying could reuse the same sequence and corrupt replay.
		j.poisoned = true
		return fmt.Errorf("sync journal event: %w: %w", err, ErrJournalPoisoned)
	}
	return nil
}

func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}

func (j *Journal) Path() string { return j.path }
