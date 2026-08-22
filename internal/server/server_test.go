//nolint:cyclop // package average includes out-of-scope production functions.
package server

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
)

type memoryAppender struct{}

func (memoryAppender) Append(protocol.Event) error { return nil }

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestUnixSocketRoundTrip(t *testing.T) {
	engine, err := state.New(memoryAppender{}, nil, state.Options{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bridge.sock")
	service := New(engine, path)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Serve(ctx) }()
	waitForSocket(t, path)

	bridge := client.New(path)
	var registered protocol.Actor
	if err := bridge.Call(context.Background(), "actor.register", protocol.RegisterParams{Actor: protocol.Actor{
		Address: "21234567-89ab-4def-8123-456789abcdef", Harness: "pi", SessionUUID: "21234567-89ab-4def-8123-456789abcdef", CWD: "/repo",
	}}, &registered); err != nil {
		t.Fatal(err)
	}
	if registered.Address != "21234567-89ab-4def-8123-456789abcdef" {
		t.Fatalf("registered = %#v", registered)
	}
	var sessions struct {
		Actors []protocol.Actor `json:"actors"`
	}
	if err := bridge.Call(context.Background(), "sessions.list", map[string]any{}, &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions.Actors) != 1 || sessions.Actors[0].Address != "21234567-89ab-4def-8123-456789abcdef" {
		t.Fatalf("sessions = %#v", sessions.Actors)
	}

	// A future streaming subscriber may hold a connection open. Shutdown must
	// close active clients instead of waiting forever for their scanners.
	idle, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := idle.Close(); err != nil {
			t.Errorf("close idle: %v", err)
		}
	}()
	var stopping struct {
		Stopping bool `json:"stopping"`
	}
	if err := bridge.Call(context.Background(), "daemon.shutdown", map[string]any{}, &stopping); err != nil {
		t.Fatal(err)
	}
	if !stopping.Stopping {
		t.Fatal("shutdown response did not report stopping")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err == nil {
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, err := json.Marshal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("socket %s did not become available", data)
}
