package provenance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

// CheckpointIdentity is the deterministic evidence boundary. Extractor
// configuration deliberately does not participate in this identity.
type CheckpointIdentity struct {
	RepositoryUUID    string `json:"repository_uuid"`
	WorkspaceUUID     string `json:"workspace_uuid"`
	Actor             string `json:"actor"`
	SessionGeneration uint64 `json:"session_generation"`
	JournalStart      uint64 `json:"journal_start_sequence"`
	JournalEnd        uint64 `json:"journal_end_sequence"`
	CheckpointKind    string `json:"checkpoint_kind"`
	EvidenceHash      string `json:"evidence_hash"`
}

func CheckpointID(request protocol.CheckpointRequest, evidence any) (string, error) {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return "", fmt.Errorf("encode checkpoint evidence: %w", err)
	}
	evidenceSum := sha256.Sum256(encoded)
	identity := CheckpointIdentity{
		RepositoryUUID: request.RepositoryUUID, WorkspaceUUID: request.WorkspaceUUID, Actor: request.Actor,
		SessionGeneration: request.SessionGeneration, JournalStart: request.JournalStart,
		JournalEnd: request.JournalEnd, CheckpointKind: request.CheckpointKind,
		EvidenceHash: hex.EncodeToString(evidenceSum[:]),
	}
	canonical, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode checkpoint identity: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return "checkpoint:" + hex.EncodeToString(sum[:]), nil
}

func ExtractionRunID(checkpointID, model, promptVersion, schemaVersion string) string {
	identity := struct {
		CheckpointID  string `json:"checkpoint_id"`
		Model         string `json:"model"`
		PromptVersion string `json:"prompt_version"`
		SchemaVersion string `json:"schema_version"`
	}{checkpointID, model, promptVersion, schemaVersion}
	encoded, _ := json.Marshal(identity)
	sum := sha256.Sum256(encoded)
	return "extraction:" + hex.EncodeToString(sum[:])
}
