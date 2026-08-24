package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type syncFailureFile struct {
	writes int
}

func (f *syncFailureFile) Write(value []byte) (int, error) {
	f.writes++
	return len(value), nil
}
func (f *syncFailureFile) Sync() error  { return errors.New("simulated sync failure") }
func (f *syncFailureFile) Close() error { return nil }

func TestJournalPoisonsAfterAmbiguousSyncFailure(t *testing.T) {
	file := &syncFailureFile{}
	journal := &Journal{file: file}
	event := protocol.Event{Version: 1, Sequence: 1, Type: "test", At: time.Now(), Data: json.RawMessage(`{}`)}
	if err := journal.Append(event); !errors.Is(err, ErrJournalPoisoned) {
		t.Fatalf("sync error = %v, want ErrJournalPoisoned", err)
	}
	if err := journal.Append(event); !errors.Is(err, ErrJournalPoisoned) {
		t.Fatalf("retry error = %v, want ErrJournalPoisoned", err)
	}
	if file.writes != 1 {
		t.Fatalf("poisoned journal performed %d writes, want 1", file.writes)
	}
}

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestJournalCanDiscardAlreadyProjectedExternalPayloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	journal, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		event := protocol.Event{Version: protocol.Version, Sequence: sequence, Type: "external_change.observed", At: time.Now(), Data: json.RawMessage(`{"path":"/repo/file"}`)}
		if err := journal.Append(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, events, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened journal: %v", err)
		}
	})
	if len(events) != 2 || events[0].Data != nil || len(events[1].Data) == 0 {
		t.Fatalf("discarded replay payloads = %#v", events)
	}
}

//nolint:cyclop // Recovery test intentionally keeps crash-tail setup and replay assertions together.
func TestJournalReplaysAndTruncatesPartialCrashTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "events.jsonl")
	journal, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]string{"address": "a"})
	if err != nil {
		t.Fatal(err)
	}
	event := protocol.Event{Version: 1, Sequence: 1, Type: "actor.upserted", At: time.Now(), Data: data}
	if err := journal.Append(event); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"version":1,"sequence":2`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Errorf("close file: %v", err)
	}

	reopened, events, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened: %v", err)
		}
	}()
	if len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("events = %#v", events)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(content)) || content[len(content)-1] != '\n' {
		t.Fatalf("partial tail was not truncated: %q", content)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("journal mode = %o", info.Mode().Perm())
	}
	directoryInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("journal directory mode = %o", directoryInfo.Mode().Perm())
	}
}
