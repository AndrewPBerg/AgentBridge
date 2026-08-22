package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareTreesReportsMatchingFiles(t *testing.T) {
	t.Parallel()
	source := t.TempDir()
	target := t.TempDir()
	for _, root := range []string{source, target} {
		require.NoError(t, os.WriteFile(filepath.Join(root, "one.txt"), []byte("same"), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(root, "two.txt"), []byte("same too"), 0o600))
	}

	report := &doctorReport{}
	compareTrees(report, "test", source, target, []string{"one.txt", "two.txt"})

	assert.False(t, report.failed)
	require.Len(t, report.checks, 1)
	assert.Equal(t, doctorCheck{Status: "ok", Name: "test", Detail: "installed files match source"}, report.checks[0])
}

func TestCompareTreesReportsMissingAndChangedFiles(t *testing.T) {
	t.Parallel()
	source := t.TempDir()
	target := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "changed.txt"), []byte("source"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(target, "changed.txt"), []byte("target"), 0o600))

	report := &doctorReport{}
	compareTrees(report, "test", source, target, []string{"changed.txt", "missing.txt"})

	assert.True(t, report.failed)
	require.Len(t, report.checks, 1)
	assert.Equal(t, "fail", report.checks[0].Status)
	assert.Contains(t, report.checks[0].Detail, "changed.txt")
	assert.Contains(t, report.checks[0].Detail, "missing.txt")
}
