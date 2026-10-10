package badger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	common "github.com/osiloke/gostore/common"
)

// LockDecision represents the outcome of a lock evaluation by a LockResolver.
type LockDecision int

const (
	// LockHeld indicates the database lock is actively held by a live process.
	LockHeld LockDecision = iota
	// LockStale indicates the lock is abandoned/stale and safe to remove.
	LockStale
	// LockUnknown indicates the status cannot be determined; treated as LockHeld (fail-safe).
	LockUnknown
)

// LockInfo encapsulates details about a detected LOCK file.
type LockInfo struct {
	Path    string
	PID     int
	ModTime time.Time
	Owner   []byte
}

// LockResolver is an interface implemented by environment/platform-aware checkers
// to determine whether an existing database lock is still active or stale.
type LockResolver interface {
	Resolve(ctx context.Context, info LockInfo) (LockDecision, error)
}

// LockedError represents a failure to acquire the database lock, wrapping common.ErrDatabaseLocked
// while providing detailed diagnostic LockInfo.
type LockedError struct {
	Info LockInfo
	Err  error
}

func (e *LockedError) Error() string {
	if e.Info.PID > 0 {
		return fmt.Sprintf("%v: lock held at %s (recorded pid: %d, mtime: %s)", common.ErrDatabaseLocked, e.Info.Path, e.Info.PID, e.Info.ModTime.Format(time.RFC3339))
	}
	return fmt.Sprintf("%v: lock held at %s (mtime: %s)", common.ErrDatabaseLocked, e.Info.Path, e.Info.ModTime.Format(time.RFC3339))
}

// Unwrap returns common.ErrDatabaseLocked so errors.Is(err, common.ErrDatabaseLocked) returns true.
func (e *LockedError) Unwrap() error {
	return common.ErrDatabaseLocked
}

// Is supports errors.Is matching against common.ErrDatabaseLocked.
func (e *LockedError) Is(target error) bool {
	return target == common.ErrDatabaseLocked
}

// WithLockResolver configures a custom LockResolver on the BadgerStore.
func WithLockResolver(resolver LockResolver) StoreOpt {
	return func(s *BadgerStore) {
		s.lockResolver = resolver
	}
}

// WithContext configures a context on the BadgerStore for operations like lock resolution.
func WithContext(ctx context.Context) StoreOpt {
	return func(s *BadgerStore) {
		s.ctx = ctx
	}
}

// inspectLock reads LockInfo for a given dbPath.
func inspectLock(dbPath string) (LockInfo, bool, error) {
	lockPath := filepath.Join(dbPath, "LOCK")
	fi, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return LockInfo{}, false, nil
		}
		return LockInfo{}, false, err
	}

	info := LockInfo{
		Path:    lockPath,
		ModTime: fi.ModTime(),
	}

	// Try reading advisory PID Badger wrote to LOCK
	if data, err := os.ReadFile(lockPath); err == nil {
		pidStr := strings.TrimSpace(string(data))
		if pid, err := strconv.Atoi(pidStr); err == nil {
			info.PID = pid
		}
	}

	// Try reading optional sidecar LOCK.owner file if present
	ownerPath := filepath.Join(dbPath, "LOCK.owner")
	if ownerData, err := os.ReadFile(ownerPath); err == nil {
		info.Owner = ownerData
	}

	return info, true, nil
}

// checkLock inspects whether a LOCK file exists at dbPath. If a lock exists:
//   - If a LockResolver is configured, it invokes the resolver.
//     If the resolver decides LockStale, the LOCK file is removed and startup proceeds.
//     If LockHeld or LockUnknown (or resolver returns error), *LockedError is returned.
//   - If no LockResolver is configured, *LockedError is returned immediately.
func (s *BadgerStore) checkLock(dbPath string) error {
	info, exists, err := inspectLock(dbPath)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if s.lockResolver != nil {
		ctx := s.ctx
		if ctx == nil {
			ctx = context.Background()
		}

		decision, rErr := s.lockResolver.Resolve(ctx, info)
		if rErr != nil {
			if s.Logger != nil {
				s.Logger.Warn("lock resolver returned error, treating as held", "err", rErr.Error(), "path", info.Path)
			}
			return &LockedError{Info: info, Err: rErr}
		}

		if decision == LockStale {
			if s.Logger != nil {
				s.Logger.Warn("removing stale database lock", "path", info.Path, "pid", info.PID, "mtime", info.ModTime)
			}
			if rmErr := os.Remove(info.Path); rmErr != nil && !os.IsNotExist(rmErr) {
				if s.Logger != nil {
					s.Logger.Error("failed to remove stale lock file", "path", info.Path, "err", rmErr.Error())
				}
				return fmt.Errorf("failed to remove stale lock file %s: %w", info.Path, rmErr)
			}
			// Clean up sidecar owner file if present
			_ = os.Remove(filepath.Join(dbPath, "LOCK.owner"))
			return nil
		}
	}

	return &LockedError{Info: info}
}
