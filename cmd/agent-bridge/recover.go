package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
)

type daemonMetadata struct {
	PID          int    `json:"pid"`
	StartTicks   uint64 `json:"linux_start_ticks"`
	Executable   string `json:"executable"`
	Socket       string `json:"socket"`
	Database     string `json:"database"`
	Journal      string `json:"journal"`
	InstanceUUID string `json:"instance_uuid"`
	StartedAt    string `json:"started_at"`
}
type recoveryAction struct {
	Action string `json:"action"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}
type recoveryReport struct {
	OK      bool             `json:"ok"`
	Actions []recoveryAction `json:"actions"`
}

func removeQuietly(path string) {
	if err := os.Remove(path); err != nil {
		return
	}
}

func pidfilePath() string { return filepath.Join(stateDir(), "daemon.pid") }
func newDaemonMetadata(socket, database, journal string) (daemonMetadata, error) {
	e, err := os.Executable()
	if err != nil {
		return daemonMetadata{}, err
	}
	e, err = filepath.EvalSymlinks(e)
	if err != nil {
		return daemonMetadata{}, err
	}
	t, err := processStartTicks(os.Getpid())
	if err != nil {
		return daemonMetadata{}, err
	}
	var b [16]byte
	if _, err = rand.Read(b[:]); err != nil {
		return daemonMetadata{}, err
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	u := fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:]))
	return daemonMetadata{PID: os.Getpid(), StartTicks: t, Executable: e, Socket: socket, Database: database, Journal: journal, InstanceUUID: u, StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
}

//nolint:gocritic,cyclop // daemon metadata is intentionally passed by value at this boundary.
func writeDaemonMetadata(m daemonMetadata) error {
	p := pidfilePath()
	if existing, err := os.ReadFile(p); err == nil {
		var current daemonMetadata
		if json.Unmarshal(existing, &current) == nil && metadataOwner(current) {
			return errors.New("daemon pidfile belongs to a running process")
		}
		return errors.New("daemon pidfile already exists; recover it before serving")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".daemon.pid.*")
	if err != nil {
		return err
	}
	n := f.Name()
	defer removeQuietly(n)
	if err = f.Chmod(0o600); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if e := f.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err := os.Link(n, p); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("daemon pidfile was created concurrently")
		}
		return err
	}
	return nil
}

//nolint:gocritic // daemon metadata is intentionally passed by value at this boundary.
func removeDaemonMetadata(m daemonMetadata) {
	b, e := os.ReadFile(pidfilePath())
	if e != nil {
		return
	}
	var x daemonMetadata
	if json.Unmarshal(b, &x) == nil && x.InstanceUUID != "" && x.InstanceUUID == m.InstanceUUID {
		removeQuietly(pidfilePath())
	}
}

func processStartTicks(pid int) (uint64, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return 0, e
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, errors.New("invalid /proc stat")
	}
	f := strings.Fields(s[i+1:])
	if len(f) <= 19 {
		return 0, errors.New("invalid /proc stat")
	}
	return strconv.ParseUint(f[19], 10, 64)
}

func sameExecutable(pid int, expected string) bool {
	a, e := filepath.EvalSymlinks(fmt.Sprintf("/proc/%d/exe", pid))
	if e != nil {
		return false
	}
	x, e := filepath.EvalSymlinks(expected)
	return e == nil && a == x
}

//nolint:gocritic // daemon metadata is intentionally passed by value at this boundary.
func metadataOwner(m daemonMetadata) bool {
	t, e := processStartTicks(m.PID)
	return m.PID > 0 && m.StartTicks > 0 && e == nil && t == m.StartTicks && sameExecutable(m.PID, m.Executable)
}

//nolint:gocritic // daemon metadata is intentionally passed by value at this boundary.
func resolveMetadataOwner(m daemonMetadata) (bool, error) {
	if m.PID <= 0 || m.StartTicks == 0 {
		return false, nil //nolint:nilerr // an unreadable PID is not an owned daemon
	}
	ticks, err := processStartTicks(m.PID)
	if err != nil || ticks != m.StartTicks {
		return false, nil //nolint:nilerr // an unreadable or changed PID is not an owned daemon
	}
	if sameExecutable(m.PID, m.Executable) {
		return true, nil
	}
	if sameUser(m.PID) && processNamedAgentBridge(m.PID) && holdsDatabase(m.PID, m.Database) {
		return true, nil
	}
	return false, errors.New("refusing recovery: recorded PID is still alive but daemon ownership cannot be verified")
}

func daemonPing(s string) error {
	c, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var r any
	return client.New(s).Call(c, "ping", map[string]any{}, &r)
}

//nolint:cyclop,gocognit,nestif // recovery sequencing is intentionally kept in one transaction-like workflow.
func recoverDaemon(args []string) error {
	force := false
	for _, a := range args {
		if a != "--force" {
			return errors.New("usage: agent-bridge daemon recover [--force]")
		}
		force = true
	}
	r := recoveryReport{}
	add := func(a, s, d string) { r.Actions = append(r.Actions, recoveryAction{a, s, d}) }
	socket, database, journal := defaultSocket(), defaultDatabase(), defaultJournal()
	b, e := os.ReadFile(pidfilePath())
	var m daemonMetadata
	known := false
	if e == nil {
		if e = json.Unmarshal(b, &m); e != nil {
			return fmt.Errorf("refusing recovery: invalid pidfile: %w", e)
		}
		if m.Socket == "" || m.Database == "" || m.Journal == "" {
			return errors.New("refusing recovery: pidfile is missing daemon paths")
		}
		socket, database, journal = m.Socket, m.Database, m.Journal
	} else if !os.IsNotExist(e) {
		return e
	}
	if daemonPing(socket) == nil {
		add("ping", "ok", "daemon is healthy; no action needed")
		r.OK = true
		return printRecovery(r)
	}
	add("ping", "unreachable", socket)
	if e == nil {
		known, e = resolveMetadataOwner(m)
		if e != nil {
			return e
		}
		if known {
			add("owner", "verified", fmt.Sprintf("pid %d", m.PID))
		} else {
			add("owner", "dead", "pidfile owner is not the same process")
		}
	} else {
		xs, e := legacyOwners(database)
		if e != nil {
			return e
		}
		selected, selectErr := chooseLegacyOwner(xs)
		if selectErr != nil {
			return selectErr
		}
		if selected {
			m = xs[0]
			known = true
			add("owner", "verified", fmt.Sprintf("legacy pid %d", m.PID))
		} else {
			add("owner", "none", "no daemon owns the database")
		}
	}
	if known {
		if e := terminateOwner(m.PID, m.StartTicks, force); e != nil {
			return e
		}
		add("terminate", "ok", fmt.Sprintf("pid %d stopped", m.PID))
	}
	if b != nil {
		removeDaemonMetadata(m)
		add("pidfile", "removed", pidfilePath())
	}
	if e := removeUnreachableSocket(socket); e != nil {
		return e
	}
	add("socket", "removed", socket)
	if e := startDetachedDaemon(socket, database, journal); e != nil {
		return e
	}
	add("start", "started", "detached daemon")
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if daemonPing(socket) == nil {
			add("ping", "ok", "daemon recovered")
			r.OK = true
			return printRecovery(r)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("daemon started but did not respond to ping")
}

func chooseLegacyOwner(matches []daemonMetadata) (bool, error) {
	if len(matches) > 1 {
		return false, fmt.Errorf("refusing recovery: found %d possible legacy daemon owners", len(matches))
	}
	return len(matches) == 1, nil
}

func legacyOwners(db string) ([]daemonMetadata, error) {
	es, e := os.ReadDir("/proc")
	if e != nil {
		return nil, e
	}
	var out []daemonMetadata
	for _, x := range es {
		p, e := strconv.Atoi(x.Name())
		if e != nil || !sameUser(p) || !processNamedAgentBridge(p) || !holdsDatabase(p, db) {
			continue
		}
		ex, e := filepath.EvalSymlinks(fmt.Sprintf("/proc/%d/exe", p))
		if e != nil {
			continue
		}
		t, e := processStartTicks(p)
		if e == nil {
			out = append(out, daemonMetadata{PID: p, StartTicks: t, Executable: ex, Socket: defaultSocket(), Database: db, Journal: defaultJournal()})
		}
	}
	return out, nil
}

func sameUser(pid int) bool {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if e != nil {
		return false
	}
	for l := range strings.SplitSeq(string(b), "\n") {
		if strings.HasPrefix(l, "Uid:") {
			f := strings.Fields(l)
			return len(f) > 1 && f[1] == strconv.Itoa(os.Getuid())
		}
	}
	return false
}

func processNamedAgentBridge(pid int) bool {
	x, e := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	x = strings.TrimSuffix(x, " (deleted)")
	return e == nil && filepath.Base(x) == "agent-bridge"
}

func holdsDatabase(pid int, db string) bool {
	es, e := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if e != nil {
		return false
	}
	for _, x := range es {
		t, e := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, x.Name()))
		if e != nil {
			continue
		}
		t = strings.TrimSuffix(t, " (deleted)")
		if filepath.Clean(t) == filepath.Clean(db) || filepath.Clean(t) == filepath.Clean(db+"-wal") {
			return true
		}
	}
	return false
}

//nolint:cyclop // termination retries intentionally encode SIGTERM/SIGKILL fallback states.
func terminateOwner(pid int, ticks uint64, force bool) error {
	t, e := processStartTicks(pid)
	if e != nil || t != ticks {
		return errors.New("refusing recovery: daemon identity changed")
	}
	p, e := os.FindProcess(pid)
	if e != nil {
		return e
	}
	if e = p.Signal(syscall.SIGTERM); e != nil && !errors.Is(e, os.ErrProcessDone) {
		return fmt.Errorf("stop daemon: %w", e)
	}
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		current, currentErr := processStartTicks(pid)
		if currentErr != nil || current != ticks {
			return nil //nolint:nilerr // an exited process is a successful termination
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !force {
		return errors.New("daemon did not stop after SIGTERM; rerun with --force to permit SIGKILL")
	}
	current, currentErr := processStartTicks(pid)
	if currentErr != nil || current != ticks {
		return nil //nolint:nilerr // an exited process is a successful termination
	}
	if e := p.Signal(syscall.SIGKILL); e != nil {
		return e
	}
	until = time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		current, currentErr = processStartTicks(pid)
		if currentErr != nil || current != ticks {
			return nil //nolint:nilerr // an exited process is a successful termination
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("daemon did not exit after SIGKILL")
}

func removeUnreachableSocket(p string) error {
	i, e := os.Lstat(p)
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		return e
	}
	if i.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove non-socket path %s", p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c, e := (&net.Dialer{}).DialContext(ctx, "unix", p)
	if e == nil {
		closeQuietly(c)
		return errors.New("refusing to remove socket: daemon is responding")
	}
	return os.Remove(p)
}

func startDetachedDaemon(s, db, journal string) error {
	ex, e := os.Executable()
	if e != nil {
		return e
	}
	if e := os.MkdirAll(stateDir(), 0o700); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(stateDir(), "daemon.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if e != nil {
		return e
	}
	c := exec.CommandContext(context.Background(), ex, "serve", "--socket", s, "--journal", journal, "--database", db)
	c.Env = append(os.Environ(), "AGENT_BRIDGE_ALLOW_UNMANAGED=1")
	c.Stdout, c.Stderr = f, f
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if e = c.Start(); e != nil {
		closeQuietly(f)
		return e
	}
	if e = c.Process.Release(); e != nil {
		closeQuietly(f)
		return e
	}
	return f.Close()
}

func printRecovery(r recoveryReport) error {
	if jsonOutput {
		return printJSON(r)
	}
	for _, a := range r.Actions {
		fmt.Printf("%-10s %-10s %s\n", a.Action, a.Status, a.Detail)
	}
	return nil
}
