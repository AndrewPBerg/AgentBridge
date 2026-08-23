package main

import (
	"context"
	"encoding/json"
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

func TestDaemonLockSerializesStartupAndReleasesOnClose(t *testing.T) {
	t.Setenv("AGENT_BRIDGE_STATE_DIR", t.TempDir())
	first, err := acquireDaemonLock()
	require.NoError(t, err)
	_, err = acquireDaemonLock()
	require.ErrorContains(t, err, "startup lock is held")
	require.NoError(t, first.Close())
	second, err := acquireDaemonLock()
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestRemoveStaleDaemonMetadataPreservesLiveOwner(t *testing.T) {
	t.Setenv("AGENT_BRIDGE_STATE_DIR", t.TempDir())
	metadata, err := newDaemonMetadata("socket", "database", "journal")
	require.NoError(t, err)
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pidfilePath(), encoded, 0o600))
	require.ErrorContains(t, removeStaleDaemonMetadata(), "running process")
	_, err = os.Stat(pidfilePath())
	require.NoError(t, err)

	metadata.StartTicks++
	encoded, err = json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(pidfilePath(), encoded, 0o600))
	require.NoError(t, removeStaleDaemonMetadata())
	_, err = os.Stat(pidfilePath())
	require.ErrorIs(t, err, os.ErrNotExist)
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
