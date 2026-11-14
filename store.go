package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const cacheSchemaVersion = 2

const catalogSchema = `
CREATE TABLE IF NOT EXISTS entries (
	action_id TEXT PRIMARY KEY,
	output_id TEXT NOT NULL,
	size INTEGER NOT NULL,
	compressed_size INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	accessed_at INTEGER NOT NULL,
	blob_type INTEGER,
	blob_type_version INTEGER
);
CREATE INDEX IF NOT EXISTS entries_accessed_at ON entries(accessed_at);

CREATE TABLE IF NOT EXISTS runs (
	run_id TEXT PRIMARY KEY,
	path TEXT NOT NULL,
	lock_path TEXT NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS state (
	key TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
`

type entry struct {
	ActionID       string
	OutputID       string
	Size           int64
	CompressedSize int64
	CreatedAt      time.Time
	AccessedAt     time.Time
}

type store struct {
	config

	db                *sqliteDB
	q                 *catalog
	versionDir        string
	outputsDir        string
	liveRoot          string
	lifecycleLockPath string
	runID             string
	runDir            string
	runLock           *flock.Flock
	mu                sync.Mutex
	accessed          map[string]int64
	lastAccessFlush   time.Time

	// Test seams. newStore sets each to the real call.
	chtimes           func(name string, atime, mtime time.Time) error
	setCompressedFlag func(fd int) error
	flushAccess       func() error
}

func newStore(cfg config) (*store, error) {
	versionDir, outputsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	if err := os.MkdirAll(versionDir, 0o777); err != nil {
		return nil, fmt.Errorf("create version dir: %w", err)
	}

	var st *store
	err := withFileLock(lifecycleLockPath, func() error {
		var err error
		st, err = newStoreLocked(cfg, versionDir, outputsDir, liveRoot, lifecycleLockPath)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	return st, nil
}

func cachePaths(cfg config) (string, string, string, string) {
	versionDir := cacheVersionDir(cfg)
	return versionDir,
		filepath.Join(versionDir, "outputs"),
		filepath.Join(versionDir, "live"),
		filepath.Join(versionDir, "lifecycle.lock")
}

func cacheVersionDir(cfg config) string {
	return filepath.Join(cfg.dir, fmt.Sprintf("v%d", cacheSchemaVersion))
}

func newStoreLocked(cfg config, versionDir, outputsDir, liveRoot, lifecycleLockPath string) (*store, error) {
	if err := os.MkdirAll(outputsDir, 0o777); err != nil {
		return nil, fmt.Errorf("create outputs dir: %w", err)
	}
	if err := os.MkdirAll(liveRoot, 0o777); err != nil {
		return nil, fmt.Errorf("create live dir: %w", err)
	}
	runDir, err := os.MkdirTemp(liveRoot, "run-")
	if err != nil {
		return nil, fmt.Errorf("create live run dir: %w", err)
	}
	runID := filepath.Base(runDir)
	runLock := flock.New(filepath.Join(runDir, "run.lock"))
	if err := runLock.Lock(); err != nil {
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("lock live run: %w", err)
	}
	db, err := openDB(filepath.Join(versionDir, "cache.db"))
	if err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}

	st := &store{
		config:            cfg,
		db:                db,
		q:                 newCatalog(db),
		versionDir:        versionDir,
		outputsDir:        outputsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
		runID:             runID,
		runDir:            runDir,
		runLock:           runLock,
		accessed:          make(map[string]int64),
		lastAccessFlush:   time.Now(),
		chtimes:           os.Chtimes,
		setCompressedFlag: setUFCompressed,
	}
	st.flushAccess = st.flushAccessTimes
	if err := st.registerRun(); err != nil {
		_ = db.Close()
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	return st, nil
}

func (st *store) withLifecycleLock(fn func() error) error {
	return withFileLock(st.lifecycleLockPath, fn)
}

func withFileLock(path string, fn func() error) error {
	lock := flock.New(path)
	if err := lock.Lock(); err != nil {
		_ = lock.Close()
		return fmt.Errorf("lock cache lifecycle: %w", err)
	}

	var err error
	err = errors.Join(err, fn())
	if unlockErr := lock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock cache lifecycle: %w", unlockErr))
	}
	if closeErr := lock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close cache lifecycle lock: %w", closeErr))
	}
	return err
}

func initDB(db *sqliteDB) error {
	return db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		version, err := queryInt64(conn, `PRAGMA user_version`, nil)
		if err != nil {
			return fmt.Errorf("read catalog version: %w", err)
		}
		if version != 0 && version != cacheSchemaVersion {
			return fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
		}
		if err := sqlitex.ExecuteScript(conn, catalogSchema, nil); err != nil {
			return fmt.Errorf("initialize catalog: %w", err)
		}
		if _, found, err := stateValue(conn, compressedSizeStateKey); err != nil {
			return fmt.Errorf("inspect compressed size state: %w", err)
		} else if !found {
			if _, err := reconcileCompressedSize(conn); err != nil {
				return fmt.Errorf("initialize compressed size state: %w", err)
			}
		}
		if err := migrateSchema(conn); err != nil {
			return err
		}
		if err := execute(conn, fmt.Sprintf(`PRAGMA user_version = %d`, cacheSchemaVersion)); err != nil {
			return fmt.Errorf("write catalog version: %w", err)
		}
		return nil
	})
}

// migrateSchema applies in-place schema changes to caches created by earlier
// versions of gocachez without bumping cacheSchemaVersion, so existing caches
// keep working after an upgrade.
func migrateSchema(conn *sqlite.Conn) error {
	for _, col := range []struct{ name, ddl string }{
		{"blob_type", "blob_type INTEGER"},
		{"blob_type_version", "blob_type_version INTEGER"},
	} {
		has, err := entriesHasColumn(conn, col.name)
		if err != nil {
			return fmt.Errorf("inspect entries schema: %w", err)
		}
		if !has {
			if err := execute(conn, "ALTER TABLE entries ADD COLUMN "+col.ddl); err != nil {
				return fmt.Errorf("add entries.%s column: %w", col.name, err)
			}
		}
	}
	// Put compressed_size immediately after output_id so per-output MAX queries
	// seek to the maximum instead of scanning every action under the write lock.
	// The remaining columns keep status queries covering.
	current, err := statusCoverIndexCurrent(conn)
	if err != nil {
		return fmt.Errorf("inspect entries_output_cover index: %w", err)
	}
	if !current {
		if err := execute(conn, `DROP INDEX IF EXISTS entries_output_cover`); err != nil {
			return fmt.Errorf("drop stale entries_output_cover index: %w", err)
		}
		if err := execute(conn, `CREATE INDEX entries_output_cover ON entries(output_id, compressed_size, size, blob_type, blob_type_version)`); err != nil {
			return fmt.Errorf("create entries_output_cover index: %w", err)
		}
	}
	if err := execute(conn, `DROP INDEX IF EXISTS entries_output_id`); err != nil {
		return fmt.Errorf("drop entries_output_id index: %w", err)
	}
	return nil
}

func statusCoverIndexCurrent(conn *sqlite.Conn) (bool, error) {
	want := []string{
		"output_id",
		"compressed_size",
		"size",
		"blob_type",
		"blob_type_version",
	}
	var got []string
	err := sqlitex.Execute(conn, `SELECT name FROM pragma_index_info('entries_output_cover') ORDER BY seqno`, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			got = append(got, stmt.ColumnText(0))
			return nil
		},
	})
	if err != nil {
		return false, err
	}
	return slices.Equal(got, want), nil
}

func (st *store) close() {
	flushErr := st.flushAccess()
	if flushErr != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", flushErr)
	}
	if err := st.unregisterRun(); err != nil && st.verbose {
		log.Printf("gocachez: unregister run failed: %v", err)
	}
	// After a failed flush the catalog holds stale last-use times, and the floor
	// reads them. Skip the prune; the next idle close prunes. The unflushed times
	// live only in memory and are lost at exit, so a later helper's idle close can
	// still prune those outputs inside the hour.
	if flushErr == nil {
		if err := st.pruneAutomatically(time.Now()); err != nil && st.verbose {
			log.Printf("gocachez: prune failed: %v", err)
		}
	}
	if err := st.db.Close(); err != nil && st.verbose {
		log.Printf("gocachez: close catalog failed: %v", err)
	}
}

func (st *store) registerRun() error {
	now := unixMillis(time.Now())
	if err := st.q.registerRun(context.Background(), st.runID, st.runDir, st.runLock.Path(), now); err != nil {
		return fmt.Errorf("register run: %w", err)
	}
	return nil
}

func (st *store) unregisterRun() error {
	var err error
	if deleteErr := st.q.deleteRun(context.Background(), st.runID); deleteErr != nil {
		err = errors.Join(err, fmt.Errorf("delete run record: %w", deleteErr))
	}
	if unlockErr := st.runLock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock live run: %w", unlockErr))
	}
	if closeErr := st.runLock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close live run lock: %w", closeErr))
	}
	if removeErr := os.RemoveAll(st.runDir); removeErr != nil {
		err = errors.Join(err, fmt.Errorf("remove live run dir: %w", removeErr))
	}
	return err
}

func (st *store) cleanupAbandonedRuns() error {
	runs, err := st.q.listOtherRuns(context.Background(), st.runID)
	if err != nil {
		return fmt.Errorf("query runs: %w", err)
	}

	for _, run := range runs {
		reclaimed, err := st.tryReclaimRun(run.runID, run.path, run.lockPath)
		if err != nil {
			return err
		}
		if reclaimed && st.verbose {
			log.Printf("gocachez: reclaimed abandoned live run %s", run.runID)
		}
	}
	return nil
}

func (st *store) tryReclaimRun(runID, runDir, lockPath string) (bool, error) {
	if _, err := os.Stat(runDir); errors.Is(err, os.ErrNotExist) {
		if err := st.q.deleteRun(context.Background(), runID); err != nil {
			return false, fmt.Errorf("delete missing-run record: %w", err)
		}
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat live run dir: %w", err)
	}

	runLock := flock.New(lockPath)
	locked, err := runLock.TryLock()
	if err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("try lock live run %s: %w", runID, err)
	}
	if !locked {
		_ = runLock.Close()
		return false, nil
	}

	if err := st.q.deleteRun(context.Background(), runID); err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		return false, fmt.Errorf("delete abandoned run record: %w", err)
	}
	if err := runLock.Unlock(); err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("unlock abandoned live run: %w", err)
	}
	if err := runLock.Close(); err != nil {
		return false, fmt.Errorf("close abandoned live run lock: %w", err)
	}
	if err := os.RemoveAll(runDir); err != nil {
		return false, fmt.Errorf("remove abandoned live run dir: %w", err)
	}
	return true, nil
}
