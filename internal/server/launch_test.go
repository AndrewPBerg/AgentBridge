package server

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
)

// TestLaunchAttachmentStressOverUnixSocket simulates a Pi parent launching
// several Luna sessions: each child presents only its own actor identity and
// the explicit launch UUID during registration. Parentage is never inferred.
//
//nolint:cyclop,gocognit // The socket stress journey intentionally covers setup, concurrency, replay-visible reads, and shutdown.
func TestLaunchAttachmentStressOverUnixSocket(t *testing.T) {
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

	parents := []string{"11234567-89ab-4def-8123-456789abcdef", "21234567-89ab-4def-8123-456789abcdef"}
	for _, parent := range parents {
		registerLaunchTestActor(t, bridge, parent, "")
	}

	launches := []string{
		"31234567-89ab-4def-8123-456789abcdef",
		"41234567-89ab-4def-8123-456789abcdef",
		"51234567-89ab-4def-8123-456789abcdef",
		"61234567-89ab-4def-8123-456789abcdef",
	}
	children := []string{
		"71234567-89ab-4def-8123-456789abcdef",
		"81234567-89ab-4def-8123-456789abcdef",
		"91234567-89ab-4def-8123-456789abcdef",
		"a1234567-89ab-4def-8123-456789abcdef",
	}
	for _, launchUUID := range launches {
		var created protocol.Launch
		if err := bridge.Call(t.Context(), "launch.create", protocol.LaunchCreateParams{LaunchUUID: launchUUID, ParentActors: []string{parents[1], parents[0]}}, &created); err != nil {
			t.Fatalf("create %s: %v", launchUUID, err)
		}
		if len(created.ParentActors) != 2 || created.ParentActors[0] != parents[0] || created.ParentActors[1] != parents[1] {
			t.Fatalf("created launch = %#v", created)
		}
	}

	var group sync.WaitGroup
	errors := make(chan error, len(launches))
	for index, launchUUID := range launches {
		group.Add(1)
		go func(launchUUID, child string) {
			defer group.Done()
			var registered protocol.Actor
			errors <- bridge.Call(t.Context(), "actor.register", protocol.RegisterParams{Actor: protocol.Actor{Address: child, SessionUUID: child, Harness: "pi", CWD: "/repo"}, LaunchUUID: launchUUID}, &registered)
		}(launchUUID, children[index])
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	for index, launchUUID := range launches {
		var launch protocol.Launch
		if err := bridge.Call(t.Context(), "launch.get", map[string]string{"launch_uuid": launchUUID}, &launch); err != nil {
			t.Fatalf("get %s: %v", launchUUID, err)
		}
		wantChild := children[index]
		if launch.ChildActor != wantChild || launch.ChildAttachedAt == nil || len(launch.ParentActors) != 2 {
			t.Fatalf("launch %s = %#v", launchUUID, launch)
		}
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
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

func registerLaunchTestActor(t *testing.T, bridge *client.Client, uuid, launchUUID string) {
	t.Helper()
	var actor protocol.Actor
	if err := bridge.Call(t.Context(), "actor.register", protocol.RegisterParams{Actor: protocol.Actor{Address: uuid, SessionUUID: uuid, Harness: "pi", CWD: "/repo"}, LaunchUUID: launchUUID}, &actor); err != nil {
		t.Fatal(err)
	}
}
