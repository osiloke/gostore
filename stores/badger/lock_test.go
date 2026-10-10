package badger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	common "github.com/osiloke/gostore/common"
	"github.com/stretchr/testify/assert"
)

type mockResolver struct {
	decision LockDecision
	err      error
	called   bool
	lastInfo LockInfo
}

func (m *mockResolver) Resolve(ctx context.Context, info LockInfo) (LockDecision, error) {
	m.called = true
	m.lastInfo = info
	return m.decision, m.err
}

func TestLockResolver_DefaultWithoutResolver(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-lock-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	assert.NoError(t, os.MkdirAll(dbDir, 0700))

	// Create a dummy LOCK file
	lockPath := filepath.Join(dbDir, "LOCK")
	assert.NoError(t, os.WriteFile(lockPath, []byte("12345\n"), 0644))

	// Without a resolver, New should fail with ErrDatabaseLocked
	store, err := New(tmpDir)
	assert.Nil(t, store)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, common.ErrDatabaseLocked), "expected error to match ErrDatabaseLocked")

	var lockedErr *LockedError
	assert.True(t, errors.As(err, &lockedErr))
	assert.Equal(t, 12345, lockedErr.Info.PID)
	assert.Equal(t, lockPath, lockedErr.Info.Path)
}

func TestLockResolver_Held(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-lock-held-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	assert.NoError(t, os.MkdirAll(dbDir, 0700))

	lockPath := filepath.Join(dbDir, "LOCK")
	assert.NoError(t, os.WriteFile(lockPath, []byte("9999\n"), 0644))

	res := &mockResolver{decision: LockHeld}
	store, err := New(tmpDir, WithLockResolver(res))
	assert.Nil(t, store)
	assert.Error(t, err)
	assert.True(t, res.called)
	assert.True(t, errors.Is(err, common.ErrDatabaseLocked))
	// Lock file must still exist
	_, statErr := os.Stat(lockPath)
	assert.NoError(t, statErr)
}

func TestLockResolver_Unknown(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-lock-unknown-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	assert.NoError(t, os.MkdirAll(dbDir, 0700))

	lockPath := filepath.Join(dbDir, "LOCK")
	assert.NoError(t, os.WriteFile(lockPath, []byte("9999\n"), 0644))

	res := &mockResolver{decision: LockUnknown}
	store, err := New(tmpDir, WithLockResolver(res))
	assert.Nil(t, store)
	assert.Error(t, err)
	assert.True(t, res.called)
	assert.True(t, errors.Is(err, common.ErrDatabaseLocked))
	// Lock file must still exist
	_, statErr := os.Stat(lockPath)
	assert.NoError(t, statErr)
}

func TestLockResolver_ResolverError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-lock-err-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	assert.NoError(t, os.MkdirAll(dbDir, 0700))

	lockPath := filepath.Join(dbDir, "LOCK")
	assert.NoError(t, os.WriteFile(lockPath, []byte("9999\n"), 0644))

	res := &mockResolver{err: fmt.Errorf("network error")}
	store, err := New(tmpDir, WithLockResolver(res))
	assert.Nil(t, store)
	assert.Error(t, err)
	assert.True(t, res.called)
	assert.True(t, errors.Is(err, common.ErrDatabaseLocked))
	// Lock file must still exist
	_, statErr := os.Stat(lockPath)
	assert.NoError(t, statErr)
}

func TestLockResolver_Stale(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-lock-stale-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbDir := filepath.Join(tmpDir, "db")
	assert.NoError(t, os.MkdirAll(dbDir, 0700))

	lockPath := filepath.Join(dbDir, "LOCK")
	assert.NoError(t, os.WriteFile(lockPath, []byte("4321\n"), 0644))
	ownerPath := filepath.Join(dbDir, "LOCK.owner")
	assert.NoError(t, os.WriteFile(ownerPath, []byte(`{"service":"test"}`), 0644))

	res := &mockResolver{decision: LockStale}
	store, err := New(tmpDir, WithLockResolver(res))
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer store.Close()

	assert.True(t, res.called)
	assert.Equal(t, 4321, res.lastInfo.PID)
	assert.Equal(t, `{"service":"test"}`, string(res.lastInfo.Owner))

	// Old LOCK.owner should have been cleaned up
	_, statErr := os.Stat(ownerPath)
	assert.True(t, os.IsNotExist(statErr))
}
