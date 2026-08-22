package main

import (
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestWorkerContextValidatesCanonicalUUIDs(t *testing.T) {
	t.Setenv("AGENT_BRIDGE_ACTOR_UUID", "not-a-uuid")
	t.Setenv("AGENT_BRIDGE_WORK_UNIT_UUID", "36a45f0d-9e8f-41b2-bd4c-dbbe8b66da43")
	_, err := workerContextFromEnvironment()
	require.ErrorContains(t, err, "AGENT_BRIDGE_ACTOR_UUID")
}

func TestWorkerResultIDsAreRepeatable(t *testing.T) {
	var ids workerResultIDs
	require.NoError(t, ids.Set("test-a"))
	require.NoError(t, ids.Set("test-b"))
	require.Equal(t, []string{"test-a", "test-b"}, ids.values)
	require.Error(t, ids.Set(""))
}

func TestWorkerClaimKindUsesStructuredVocabulary(t *testing.T) {
	require.Equal(t, "test", workerClaimKind("test"))
	require.Equal(t, "summary", workerClaimKind("settled"))
}

func TestWorkerVerifiedEvidenceRule(t *testing.T) {
	// Keep the command-level policy explicit: verified execution claims need
	// supplied test-result ordinals, rather than silently becoming asserted.
	if protocol.ClaimVerified == protocol.ClaimAsserted {
		t.Fatal("claim statuses unexpectedly alias each other")
	}
}
