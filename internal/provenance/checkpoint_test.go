package provenance

import (
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointIDIsStableAndIndependentOfExtractor(t *testing.T) {
	request := protocol.CheckpointRequest{Actor: "a", RepositoryUUID: "a", WorkspaceUUID: "a", SessionGeneration: 2, JournalStart: 4, JournalEnd: 9, CheckpointKind: "settled"}
	left, err := CheckpointID(request, []any{"evidence", 1})
	if err != nil {
		t.Fatal(err)
	}
	right, err := CheckpointID(request, []any{"evidence", 1})
	if err != nil || left != right {
		t.Fatalf("unstable checkpoint id: %q %q %v", left, right, err)
	}
	if left == ExtractionRunID(left, "model-a", "prompt-1", "schema-1") || left == ExtractionRunID(left, "model-b", "prompt-2", "schema-2") {
		t.Fatal("checkpoint id unexpectedly overlaps extraction id")
	}
	if ExtractionRunID(left, "model-a", "prompt-1", "schema-1") == ExtractionRunID(left, "model-b", "prompt-2", "schema-2") {
		t.Fatal("extractor versions did not distinguish extraction runs")
	}
}
