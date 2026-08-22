package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
)

type doctorCheck struct {
	Status string `json:"status"`
	Name   string `json:"name"`
	Detail string `json:"detail"`
}

type doctorReport struct {
	failed bool
	checks []doctorCheck
}

type doctorFailure struct{}

func (doctorFailure) Error() string { return "doctor found deployment problems" }

func doctor(args []string) error {
	if len(args) != 0 {
		return errors.New("usage: agent-bridge doctor")
	}

	report := doctorReport{}
	report.checkBinary()
	report.checkPath("state directory", stateDir(), true)
	report.checkPath("journal", defaultJournal(), false)
	report.checkPath("database", defaultDatabase(), false)
	report.checkSocket(defaultSocket())
	report.checkDeployment()
	if jsonOutput {
		if err := printJSON(map[string]any{"ok": !report.failed, "checks": report.checks}); err != nil {
			return err
		}
	}
	if report.failed {
		return doctorFailure{}
	}
	return nil
}

func (r *doctorReport) checkBinary() {
	executable, err := os.Executable()
	if err != nil {
		r.fail("binary", fmt.Sprintf("cannot determine executable: %v", err))
		return
	}
	info, err := os.Stat(executable)
	if err != nil {
		r.fail("binary", fmt.Sprintf("cannot inspect executable: %v", err))
		return
	}
	version := "devel"
	if build, ok := debug.ReadBuildInfo(); ok && build.Main.Version != "" {
		version = build.Main.Version
	}
	r.ok("binary", fmt.Sprintf("%s (%s)", executable, version))
	if info.Mode().Perm()&0o111 == 0 {
		r.fail("binary", "executable is not marked executable")
	}
}

func (r *doctorReport) checkPath(label, path string, directory bool) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			r.warn(label, fmt.Sprintf("missing: %s", path))
			return
		}
		r.fail(label, fmt.Sprintf("cannot inspect %s: %v", path, err))
		return
	}
	if directory && !info.IsDir() {
		r.fail(label, fmt.Sprintf("not a directory: %s", path))
		return
	}
	if !directory && info.IsDir() {
		r.fail(label, fmt.Sprintf("is a directory: %s", path))
		return
	}
	if info.Mode().Perm()&0o077 != 0 {
		r.fail(label, fmt.Sprintf("permissions are %o, expected owner-only: %s", info.Mode().Perm(), path))
		return
	}
	r.ok(label, path)
}

func (r *doctorReport) checkSocket(path string) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			r.warn("daemon", "socket is missing: "+path)
			return
		}
		r.fail("daemon", fmt.Sprintf("cannot inspect socket: %v", err))
		return
	}
	if info.Mode()&os.ModeSocket == 0 {
		r.fail("daemon", "socket path is not a Unix socket: "+path)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var result any
	if err := client.New(path).Call(ctx, "ping", map[string]any{}, &result); err != nil {
		r.fail("daemon", fmt.Sprintf("socket exists but ping failed: %v", err))
		return
	}
	r.ok("daemon", "responding on "+path)
}

func (r *doctorReport) checkDeployment() {
	sourceRoot := sourceRoot()
	if sourceRoot == "" {
		r.warn("Pi deployment", "source tree not found; set AGENT_BRIDGE_SOURCE_DIR to compare installed files")
		return
	}
	piHome := os.Getenv("PI_HOME")
	if piHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			r.warn("Pi deployment", "cannot determine home directory")
			return
		}
		piHome = filepath.Join(home, ".pi")
	}
	targetRoot := filepath.Join(piHome, "agent")
	compareTrees(r, "Pi adapter", filepath.Join(sourceRoot, "packages/pi-extension"), filepath.Join(targetRoot, "extensions", "agent-bridge"), []string{
		"client.ts", "git.ts", "herdr.ts", "index.ts", "intent.ts", "jj.ts", "provenance.ts", "protocol.ts", "talk-modal.ts", "README.md",
	})
	compareTrees(r, "Pi skill", filepath.Join(sourceRoot, "skills/agent-bridge"), filepath.Join(targetRoot, "skills", "agent-bridge"), []string{
		"SKILL.md", "references/provenance.md",
	})
}

func sourceRoot() string {
	if value := os.Getenv("AGENT_BRIDGE_SOURCE_DIR"); value != "" {
		return value
	}
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	for current := cwd; current != filepath.Dir(current); current = filepath.Dir(current) {
		if _, err := os.Stat(filepath.Join(current, "packages/pi-extension")); err == nil {
			return current
		}
	}
	return ""
}

func compareTrees(r *doctorReport, label, source, target string, files []string) {
	mismatches := make([]string, 0)
	for _, name := range files {
		sourceHash, sourceErr := fileHash(filepath.Join(source, name))
		targetHash, targetErr := fileHash(filepath.Join(target, name))
		if sourceErr != nil || targetErr != nil || sourceHash != targetHash {
			mismatches = append(mismatches, name)
		}
	}
	if len(mismatches) > 0 {
		r.fail(label, fmt.Sprintf("installed files differ or are missing: %s", strings.Join(mismatches, ", ")))
		return
	}
	r.ok(label, "installed files match source")
}

func fileHash(path string) ([sha256.Size]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return [sha256.Size]byte{}, err
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

func (r *doctorReport) ok(label, detail string) {
	r.checks = append(r.checks, doctorCheck{Status: "ok", Name: label, Detail: detail})
	if !jsonOutput {
		fmt.Printf("OK   %-14s %s\n", label, detail)
	}
}

func (r *doctorReport) warn(label, detail string) {
	r.checks = append(r.checks, doctorCheck{Status: "warn", Name: label, Detail: detail})
	if !jsonOutput {
		fmt.Printf("WARN %-14s %s\n", label, detail)
	}
}

func (r *doctorReport) fail(label, detail string) {
	r.failed = true
	r.checks = append(r.checks, doctorCheck{Status: "fail", Name: label, Detail: detail})
	if !jsonOutput {
		fmt.Printf("FAIL %-14s %s\n", label, detail)
	}
}
