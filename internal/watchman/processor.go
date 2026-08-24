package watchman

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type coordination interface {
	ObserveExternalChange(protocol.ExternalChange) (protocol.ExternalChange, error)
	HasActiveIntent(string, string) bool
	MatchIntentTransition(string, string, *protocol.FileSnapshot, *protocol.FileSnapshot, time.Time, time.Time) []string
	NotifyExternalChange(protocol.ExternalChange) error
	WatchContinuityLost(protocol.WatchContinuity) (protocol.WatchContinuity, error)
	WatchContinuityRestored(protocol.WatchContinuity) (protocol.WatchContinuity, error)
}

type baselineEntry struct {
	snapshot   protocol.FileSnapshot
	observedAt time.Time
}

var ignoredPathParts = map[string]struct{}{
	".agent-bridge": {},
	".cache":        {},
	".git":          {},
	".jj":           {},
	".mypy_cache":   {},
	".next":         {},
	".pytest_cache": {},
	".ruff_cache":   {},
	".turbo":        {},
	".venv":         {},
	"__pycache__":   {},
	"build":         {},
	"coverage":      {},
	"dist":          {},
	"node_modules":  {},
	"target":        {},
	"venv":          {},
}

type processor struct {
	mu             sync.Mutex
	coordination   coordination
	repositoryUUID string
	workspaceUUID  string
	root           string
	baseline       map[string]baselineEntry
	initialized    bool
}

func newProcessor(target coordination, actor *protocol.Actor) *processor {
	root := actor.RepositoryRoot
	if root == "" {
		root = actor.CWD
	}
	if actor.WorkspaceRoot != "" {
		root = actor.WorkspaceRoot
	}
	return &processor{coordination: target, repositoryUUID: actor.RepositoryUUID, workspaceUUID: actor.WorkspaceUUID, root: filepath.Clean(root), baseline: make(map[string]baselineEntry)}
}

func (p *processor) acceptedPath(path string) bool {
	relative, err := filepath.Rel(p.root, filepath.Clean(path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	for part := range strings.SplitSeq(filepath.ToSlash(relative), "/") {
		if _, ignored := ignoredPathParts[part]; ignored {
			return false
		}
	}
	base := filepath.Base(relative)
	return !strings.HasSuffix(base, ".log") && !strings.HasSuffix(base, ".pyc") && !strings.HasSuffix(base, ".tsbuildinfo") && base != "coverage.out"
}

func (p *processor) reconcile(paths []string, clock, continuity string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	seen := make(map[string]struct{}, len(paths))
	sort.Strings(paths)
	for _, path := range paths {
		path = filepath.Clean(path)
		if !p.acceptedPath(path) {
			continue
		}
		seen[path] = struct{}{}
		if err := p.observeLocked(path, snapshot(path), now, clock, continuity); err != nil {
			return err
		}
	}
	if p.initialized && continuity == "reconciled" {
		known := make([]string, 0, len(p.baseline))
		for path := range p.baseline {
			known = append(known, path)
		}
		sort.Strings(known)
		for _, path := range known {
			if _, ok := seen[path]; ok {
				continue
			}
			if err := p.observeLocked(path, protocol.FileSnapshot{Path: path}, now, clock, continuity); err != nil {
				return err
			}
		}
	}
	p.initialized = true
	return nil
}

func (p *processor) observe(paths []string, clock string) error {
	deadline := time.Now().Add(2 * time.Second)
	for {
		active := false
		for _, path := range paths {
			if p.coordination.HasActiveIntent(p.workspaceUUID, filepath.Clean(path)) {
				active = true
				break
			}
		}
		if !active || time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	sort.Strings(paths)
	for _, path := range paths {
		path = filepath.Clean(path)
		if !p.acceptedPath(path) {
			continue
		}
		if err := p.observeLocked(path, snapshot(path), now, clock, "current"); err != nil {
			return err
		}
	}
	return nil
}

//nolint:cyclop,gocognit,gocritic // transition classification remains explicit over an immutable snapshot value.
func (p *processor) observeLocked(path string, after protocol.FileSnapshot, now time.Time, clock, continuity string) error {
	if after.Exists && after.Kind == "directory" {
		delete(p.baseline, path)
		return nil
	}
	before, known := p.baseline[path]
	if !known {
		if !p.initialized || !after.Exists {
			if after.Exists {
				p.baseline[path] = baselineEntry{snapshot: after, observedAt: now}
			}
			return nil
		}
	}
	if known && sameSnapshot(&before.snapshot, &after) {
		p.baseline[path] = baselineEntry{snapshot: after, observedAt: now}
		return nil
	}
	if known {
		beforeSnapshot := before.snapshot
		if ids := p.coordination.MatchIntentTransition(p.workspaceUUID, path, &beforeSnapshot, &after, before.observedAt, now); len(ids) > 0 {
			if after.Exists {
				p.baseline[path] = baselineEntry{snapshot: after, observedAt: now}
			} else {
				delete(p.baseline, path)
			}
			return nil
		}
	}
	kind := "created"
	startedAt := now
	var beforeSnapshot *protocol.FileSnapshot
	if known {
		beforeValue := before.snapshot
		beforeSnapshot = &beforeValue
		startedAt = before.observedAt
		switch {
		case !after.Exists:
			kind = "deleted"
		case beforeValue.Kind != after.Kind:
			kind = "type_changed"
		default:
			kind = "modified"
		}
	}
	afterCopy := after
	change := protocol.ExternalChange{
		ID: randomUUID(), RepositoryUUID: p.repositoryUUID, WorkspaceUUID: p.workspaceUUID,
		IntervalStartedAt: startedAt, IntervalEndedAt: now, ContinuityState: continuity,
		ChangeKind: kind, Path: path, Before: beforeSnapshot, After: &afterCopy, WatchmanClock: clock,
	}
	observed, err := p.coordination.ObserveExternalChange(change)
	if err != nil {
		return err
	}
	if err := p.coordination.NotifyExternalChange(observed); err != nil {
		return err
	}
	if after.Exists {
		p.baseline[path] = baselineEntry{snapshot: after, observedAt: now}
	} else {
		delete(p.baseline, path)
	}
	return nil
}

func randomUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("random UUID: %v", err))
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}
