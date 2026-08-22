package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataOwnerRejectsPIDReuseIdentityChanges(t *testing.T) {
	metadata, err := newDaemonMetadata("socket", "database", "journal")
	require.NoError(t, err)
	require.True(t, metadataOwner(metadata))
	metadata.StartTicks++
	require.False(t, metadataOwner(metadata))
	metadata.StartTicks--
	metadata.Executable = filepath.Join(t.TempDir(), "agent-bridge")
	require.False(t, metadataOwner(metadata))
}

func TestResolveMetadataOwnerRefusesLiveIdentityMismatch(t *testing.T) {
	metadata, err := newDaemonMetadata("socket", filepath.Join(t.TempDir(), "database"), "journal")
	require.NoError(t, err)
	metadata.Executable = filepath.Join(t.TempDir(), "different-agent-bridge")
	owned, err := resolveMetadataOwner(metadata)
	require.False(t, owned)
	require.ErrorContains(t, err, "still alive")
}

func TestRemoveUnreachableSocketRemovesStaleSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.sock")
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	require.NoError(t, removeUnreachableSocket(path))
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestChooseLegacyOwnerRefusesAmbiguity(t *testing.T) {
	_, err := chooseLegacyOwner([]daemonMetadata{{PID: 1}, {PID: 2}})
	require.ErrorContains(t, err, "possible legacy daemon owners")
}

func TestRecoverCommandParsing(t *testing.T) {
	require.ErrorContains(t, recoverDaemon([]string{"--unknown"}), "usage: agent-bridge daemon recover")
}

func TestServeRefusesUnmanagedProductionLifecycle(t *testing.T) {
	t.Setenv("AGENT_BRIDGE_STATE_DIR", "")
	t.Setenv("INVOCATION_ID", "")
	t.Setenv("AGENT_BRIDGE_ALLOW_UNMANAGED", "")
	err := serve(nil)
	require.ErrorContains(t, err, "managed by systemd")
}
