package watchman

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type response struct {
	Error           string            `json:"error"`
	Warning         string            `json:"warning"`
	Watch           string            `json:"watch"`
	RelativePath    string            `json:"relative_path"`
	Clock           string            `json:"clock"`
	Subscribe       string            `json:"subscribe"`
	Subscription    string            `json:"subscription"`
	Files           []json.RawMessage `json:"files"`
	IsFreshInstance bool              `json:"is_fresh_instance"`
	Canceled        bool              `json:"canceled"`
}

type persistentClient struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	scanner *bufio.Scanner
}

func startClient(ctx context.Context, binary string, request any) (*persistentClient, error) {
	command := exec.CommandContext(ctx, binary, "-j", "--server-encoding=json", "--no-pretty", "-p")
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		stopCommand(command)
		return nil, err
	}
	if _, err := stdin.Write(encoded); err != nil {
		stopCommand(command)
		return nil, err
	}
	if err := stdin.Close(); err != nil {
		stopCommand(command)
		return nil, err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	return &persistentClient{command: command, stdin: stdin, scanner: scanner}, nil
}

func stopCommand(command *exec.Cmd) {
	if command.Process != nil {
		if err := command.Process.Kill(); err != nil {
			return
		}
	}
}

func (c *persistentClient) close() {
	if err := c.stdin.Close(); err != nil {
		return
	}
	stopCommand(c.command)
	if err := c.command.Wait(); err != nil {
		return
	}
}

func oneShot(ctx context.Context, binary string, request any) (response, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return response{}, err
	}
	command := exec.CommandContext(ctx, binary, "-j", "--server-encoding=json", "--no-pretty")
	command.Stdin = strings.NewReader(string(encoded))
	command.Stderr = io.Discard
	output, err := command.Output()
	if err != nil {
		return response{}, err
	}
	var result response
	if err := json.Unmarshal(output, &result); err != nil {
		return result, err
	}
	if result.Error != "" {
		return result, errors.New(result.Error)
	}
	return result, nil
}

func (c *persistentClient) read() (response, error) {
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return response{}, err
		}
		return response{}, io.EOF
	}
	var result response
	if err := json.Unmarshal(c.scanner.Bytes(), &result); err != nil {
		return result, err
	}
	if result.Error != "" {
		return result, errors.New(result.Error)
	}
	return result, nil
}

func queryFields() []string {
	return []string{"name", "exists", "type", "size", "mtime_ms"}
}

func querySpec(relative, since string) map[string]any {
	query := map[string]any{"fields": queryFields()}
	if relative != "" {
		query["relative_root"] = relative
	}
	if since != "" {
		query["since"] = since
	}
	return query
}

func responsePaths(root string, files []json.RawMessage) []string {
	result := make([]string, 0, len(files))
	for _, raw := range files {
		var name string
		if len(raw) > 0 && raw[0] == '"' {
			if err := json.Unmarshal(raw, &name); err != nil {
				continue
			}
		} else {
			var value struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(raw, &value); err != nil {
				continue
			}
			name = value.Name
		}
		if name == "" {
			continue
		}
		result = append(result, filepath.Join(root, filepath.FromSlash(name)))
	}
	return result
}

type workspaceWatcher struct {
	binary    string
	actor     protocol.Actor
	processor *processor
}

var errFreshInstance = errors.New("watchman fresh instance")

func (w *workspaceWatcher) run(ctx context.Context) {
	first := true
	for ctx.Err() == nil {
		if err := w.runOnce(ctx, first); err == nil {
			continue
		}
		first = false
		if ctx.Err() != nil {
			return
		}
		if _, err := w.processor.coordination.WatchContinuityLost(protocol.WatchContinuity{
			RepositoryUUID: w.actor.RepositoryUUID, WorkspaceUUID: w.actor.WorkspaceUUID, At: time.Now().UTC(),
		}); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

//nolint:cyclop,gocognit // one subscription lifecycle keeps bootstrap and stream ordering explicit.
func (w *workspaceWatcher) runOnce(ctx context.Context, first bool) error {
	project, err := oneShot(ctx, w.binary, []any{"watch-project", w.processor.root})
	if err != nil {
		return err
	}
	watchRoot := project.Watch
	if watchRoot == "" {
		return errors.New("watchman returned no watch root")
	}
	query, err := oneShot(ctx, w.binary, []any{"query", watchRoot, querySpec(project.RelativePath, "")})
	if err != nil {
		return err
	}
	continuity := "reconciled"
	if first {
		continuity = "current"
	}
	if err := w.processor.reconcile(responsePaths(w.processor.root, query.Files), query.Clock, continuity); err != nil {
		return err
	}
	_, err = w.processor.coordination.WatchContinuityRestored(protocol.WatchContinuity{
		RepositoryUUID: w.actor.RepositoryUUID, WorkspaceUUID: w.actor.WorkspaceUUID,
		At: time.Now().UTC(), WatchmanClock: query.Clock,
	})
	if err != nil {
		return err
	}
	subscription := "agent-bridge-" + strings.ReplaceAll(w.actor.WorkspaceUUID, "-", "")
	client, err := startClient(ctx, w.binary, []any{"subscribe", watchRoot, subscription, querySpec(project.RelativePath, query.Clock)})
	if err != nil {
		return err
	}
	defer client.close()
	subscribe, err := client.read()
	if err != nil {
		return err
	}
	if subscribe.Subscribe != subscription {
		return errors.New("watchman did not acknowledge subscription")
	}
	for ctx.Err() == nil {
		notification, err := client.read()
		if err != nil {
			return err
		}
		if notification.Subscription != subscription {
			continue
		}
		if notification.Canceled {
			return errors.New("watchman subscription canceled")
		}
		if notification.IsFreshInstance || strings.Contains(notification.Warning, "Recrawled") {
			return errFreshInstance
		}
		paths := responsePaths(w.processor.root, notification.Files)
		if len(paths) == 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(75 * time.Millisecond):
		}
		if err := w.processor.observe(paths, notification.Clock); err != nil {
			return err
		}
	}
	return ctx.Err()
}
