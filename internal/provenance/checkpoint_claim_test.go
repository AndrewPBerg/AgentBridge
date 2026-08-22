package provenance

import (
	"path/filepath"
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointClaimsRejectMalformedKindStatusAndOrdinal(t *testing.T) {
	base := protocol.CheckpointRequest{ID: "malformed", Actor: "01234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222", CheckpointKind: "settled"}
	cases := map[string]protocol.CheckpointClaim{
		"kind":    {Kind: "unsupported", Statement: "x", Status: protocol.ClaimAsserted},
		"status":  {Kind: "summary", Statement: "x", Status: "unknown"},
		"ordinal": {Kind: "summary", Statement: "x", Status: protocol.ClaimAsserted, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "mutation", Ordinal: -1}}},
	}
	for name, claim := range cases {
		t.Run(name, func(t *testing.T) {
			database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			request := base
			request.ID = "malformed-" + name
			request.Claims = []protocol.CheckpointClaim{claim}
			if err := database.Project(event(t, 1, "checkpoint.requested", request)); err == nil {
				t.Fatal("malformed claim was accepted")
			}
		})
	}
}

func TestCheckpointClaimLegacyMetadataBackfillsQueryableClaim(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	request := protocol.CheckpointRequest{ID: "legacy", Actor: "01234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222", CheckpointKind: "settled", Metadata: map[string]string{"summary": "legacy summary"}}
	if err := database.Project(event(t, 1, "checkpoint.requested", request)); err != nil {
		t.Fatal(err)
	}
	got, err := database.Checkpoint(request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Claims) != 1 || got.Claims[0].Kind != "summary" || got.Claims[0].Statement != "legacy summary" || got.Claims[0].Status != protocol.ClaimAsserted {
		t.Fatalf("backfilled claims = %#v", got.Claims)
	}
}

func TestCheckpointClaimVerifiedDowngradesFailedAndMissingResults(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	actor := "01234567-89ab-4def-8123-456789abcdef"
	repo := "11111111-1111-5111-8111-111111111111"
	workspace := "22222222-2222-5222-8222-222222222222"
	if _, err := database.db.Exec(`INSERT INTO test_results(id, actor, session_generation, command, cwd, completed_at, started_at, output_truncated, repository_uuid, workspace_uuid, data, event_sequence, exit_code) VALUES ('failed', ?, 1, 'go test', '/', 'now', 'now', 0, ?, ?, '{}', 2, 1)`, uuidBlob(actor), uuidBlob(repo), uuidBlob(workspace)); err != nil {
		t.Fatal(err)
	}
	request := protocol.CheckpointRequest{ID: "failed-result", Actor: actor, RepositoryUUID: repo, WorkspaceUUID: workspace, CheckpointKind: "settled", JournalStart: 1, JournalEnd: 2, TestResultIDs: []string{"failed"}, Claims: []protocol.CheckpointClaim{{Kind: "test", Statement: "passes", Status: protocol.ClaimVerified, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 0}}}}}
	if err := database.Project(event(t, 3, "checkpoint.requested", request)); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := database.db.QueryRow(`SELECT status FROM checkpoint_claims WHERE checkpoint_id='failed-result'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(protocol.ClaimAsserted) {
		t.Fatalf("failed result status = %q", status)
	}
	missing := request
	missing.ID = "missing-result"
	missing.TestResultIDs = []string{"missing"}
	if err := database.Project(event(t, 4, "checkpoint.requested", missing)); err == nil {
		t.Fatal("missing result was accepted")
	}
}

func TestCheckpointClaimQueryEvidenceOrderingAndSchemaForeignKeys(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	request := protocol.CheckpointRequest{ID: "ordering", Actor: "01234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222", CheckpointKind: "settled"}
	if err := database.Project(event(t, 1, "checkpoint.requested", request)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO checkpoint_claims(checkpoint_id, ordinal, kind, statement, status) VALUES ('ordering', 0, 'summary', 'ordered', 'asserted')`); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 0}, {Kind: "mutation", Ordinal: 0}, {Kind: "message", Ordinal: 0}} {
		if _, err := database.db.Exec(`INSERT INTO checkpoint_evidence(checkpoint_id, kind, ordinal, ref_text, ref_uuid) VALUES ('ordering', ?, ?, ?, NULL)`, ref.Kind, ref.Ordinal, ref.Kind+"-ref"); err != nil {
			t.Fatal(err)
		}
		if _, err := database.db.Exec(`INSERT INTO checkpoint_claim_evidence(checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal) VALUES ('ordering', 0, ?, ?)`, ref.Kind, ref.Ordinal); err != nil {
			t.Fatal(err)
		}
	}
	got, err := database.Checkpoint("ordering")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"message", "mutation", "test_result"}
	for i, ref := range got.Claims[0].Evidence {
		if ref.Kind != want[i] {
			t.Fatalf("evidence[%d] = %#v, want kind %q", i, ref, want[i])
		}
	}
	rows, err := database.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("schema has foreign key violations")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointClaimOutcomesPersistAndEnforceEvidence(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	actor := "01234567-89ab-4def-8123-456789abcdef"
	repo := "11111111-1111-5111-8111-111111111111"
	workspace := "22222222-2222-5222-8222-222222222222"
	zero, one := 0, 1
	results := []protocol.TestResult{
		{ID: "pass", Actor: actor, Command: "go test", ExitCode: &zero, Outcome: protocol.TestPassed, RepositoryUUID: repo, WorkspaceUUID: workspace},
		{ID: "fail", Actor: actor, Command: "go test", ExitCode: &one, Outcome: protocol.TestFailed, RepositoryUUID: repo, WorkspaceUUID: workspace},
		{ID: "blocked", Actor: actor, Command: "go test", Outcome: protocol.TestBlocked, RepositoryUUID: repo, WorkspaceUUID: workspace},
	}
	for i, result := range results {
		if err := database.Project(event(t, uint64(i+1), "test.result", result)); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := protocol.CheckpointRequest{
		ID: "outcomes", Actor: actor, RepositoryUUID: repo, WorkspaceUUID: workspace, CheckpointKind: "test", JournalStart: 1, JournalEnd: 3,
		TestResultIDs: []string{"pass", "fail", "blocked"},
		Claims: []protocol.CheckpointClaim{
			{Kind: "test", Statement: "passed", Status: protocol.ClaimVerified, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 0}}},
			{Kind: "test", Statement: "failed", Status: protocol.ClaimFailed, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 1}}},
			{Kind: "runtime", Statement: "blocked", Status: protocol.ClaimBlocked, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 2}}},
			{Kind: "build", Statement: "bare failure", Status: protocol.ClaimFailed},
		},
	}
	if err := database.Project(event(t, 4, "checkpoint.requested", checkpoint)); err != nil {
		t.Fatal(err)
	}
	got, err := database.Checkpoint(checkpoint.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []protocol.CheckpointClaimStatus{protocol.ClaimVerified, protocol.ClaimFailed, protocol.ClaimBlocked, protocol.ClaimAsserted}
	for i, status := range want {
		if got.Claims[i].Status != status {
			t.Fatalf("claim %d status = %q, want %q", i, got.Claims[i].Status, status)
		}
	}
	rows, err := database.db.Query(`SELECT outcome FROM test_results ORDER BY event_sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i, outcome := range []protocol.TestOutcome{protocol.TestPassed, protocol.TestFailed, protocol.TestBlocked} {
		if !rows.Next() {
			t.Fatalf("missing outcome row %d", i)
		}
		var stored protocol.TestOutcome
		if err := rows.Scan(&stored); err != nil || stored != outcome {
			t.Fatalf("stored outcome %d = %q, err=%v", i, stored, err)
		}
	}
	inconsistent := protocol.TestResult{ID: "bad", Actor: actor, Command: "go test", ExitCode: &zero, Outcome: protocol.TestFailed, RepositoryUUID: repo, WorkspaceUUID: workspace}
	if err := database.Project(event(t, 5, "test.result", inconsistent)); err == nil {
		t.Fatal("inconsistent outcome was projected")
	}
}

func TestCheckpointClaimEvidenceHasCompositeForeignKey(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.db.Exec(`INSERT INTO checkpoint_claim_evidence(checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal) VALUES ('missing', 0, 'test_result', 0)`); err == nil {
		t.Fatal("orphan claim evidence was accepted")
	}
}

func TestCheckpointClaimConflictIsExact(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "claims.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	base := protocol.CheckpointRequest{ID: "exact", Actor: "01234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222", CheckpointKind: "settled", Claims: []protocol.CheckpointClaim{{Kind: "summary", Statement: "one", Status: protocol.ClaimAsserted}}}
	if err := database.Project(event(t, 1, "checkpoint.requested", base)); err != nil {
		t.Fatal(err)
	}
	conflict := base
	conflict.Claims = []protocol.CheckpointClaim{{Kind: "summary", Statement: "two", Status: protocol.ClaimAsserted}}
	if err := database.Project(event(t, 2, "checkpoint.requested", conflict)); err == nil {
		t.Fatal("conflicting claim was accepted")
	}
}
