package watchman

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"syscall"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"golang.org/x/sys/unix"
)

const defaultHashLimit = int64(16 << 20)

func hashLimit() int64 {
	value := os.Getenv("AGENT_BRIDGE_HASH_MAX_BYTES")
	if value == "" {
		return defaultHashLimit
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil || limit < 0 {
		return defaultHashLimit
	}
	return limit
}

//nolint:cyclop // filesystem kinds and no-follow hashing require explicit branches.
func snapshot(path string) protocol.FileSnapshot {
	result := protocol.FileSnapshot{Path: path}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return result
	}
	if err != nil {
		result.Kind = "unreadable"
		return result
	}
	result.Exists = true
	result.Size = info.Size()
	result.ModifiedAt = info.ModTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	mode := info.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		result.Kind = "symlink"
		return result
	case mode.IsDir():
		result.Kind = "directory"
		return result
	case !mode.IsRegular():
		result.Kind = "other"
		return result
	default:
		result.Kind = "file"
	}
	if result.Size > hashLimit() {
		return result
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			result.Kind = "symlink"
		}
		return result
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		if err := unix.Close(fd); err != nil {
			return result
		}
		return result
	}
	defer func() {
		if err := file.Close(); err != nil {
			return
		}
	}()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err == nil {
		result.SHA256 = hex.EncodeToString(hash.Sum(nil))
	}
	return result
}

func sameSnapshot(left, right *protocol.FileSnapshot) bool {
	return left.Exists == right.Exists && left.Kind == right.Kind && left.SHA256 == right.SHA256 && left.Size == right.Size
}
