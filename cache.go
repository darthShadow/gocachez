package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"zombiezen.com/go/sqlite"
)

const (
	// mtimeInterval is the age-prune margin: entries unused for maxAge plus this
	// interval are pruned. The age cutoff itself is the configurable maxAge (see
	// config); the default matches GOCACHE's 5 days.
	mtimeInterval          = time.Hour
	fullPruneInterval      = time.Hour
	sizePruneTargetPercent = 90
	lastFullPruneStateKey  = "last-full-prune"
	accessFlushInterval    = 30 * time.Second
	accessFlushThreshold   = 256
	accessFlushMinInterval = time.Second

	// useFloor is how long a returned path stays after its last use. cmd/go keeps
	// the paths it got from get or put, so no prune deletes an output that was
	// used less than this long ago.
	useFloor = time.Hour

	// storedBodyLimit is the first body size the writer rejects. The resource
	// fork's offset table and the fsetxattr position are both uint32.
	storedBodyLimit = 1 << 31

	tempMarker = ".tmp-"
)

// put stores the body as one compressed file at the output's stored path and
// records the action. It writes nothing when a file of the right size is there.
func (st *store) put(req request, br *bufio.Reader) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	outputHex, err := idHex("OutputID", req.OutputID)
	if err != nil {
		return response{}, err
	}
	body, err := bodyReader(br, req.BodySize)
	if err != nil {
		return response{}, err
	}
	bodyDrained := false
	defer func() {
		if !bodyDrained {
			_ = drainBody(body)
		}
	}()
	if req.BodySize >= storedBodyLimit {
		return response{}, fmt.Errorf("put body of %d bytes: stored files must be smaller than 2 GiB", req.BodySize)
	}

	path := st.storedPath(outputHex)
	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Size() == req.BodySize {
		// The file is already there. Count the body to keep the stream in step and
		// to reject a body of the wrong size, but write nothing.
		written, err := io.Copy(io.Discard, body)
		if err != nil {
			return response{}, fmt.Errorf("read put body: %w", err)
		}
		bodyDrained = true
		if written != req.BodySize {
			return response{}, fmt.Errorf("put body size mismatch: got %d bytes, expected %d", written, req.BodySize)
		}
		return st.recordPut(req, actionHex, outputHex, path, allocatedBytes(info))
	}

	shardDir := filepath.Dir(path)
	if err := os.MkdirAll(shardDir, 0o777); err != nil {
		return response{}, fmt.Errorf("create output dir: %w", err)
	}
	tmpPath, err := st.writeStoredTemp(shardDir, outputHex, body, req.BodySize)
	if err != nil {
		return response{}, err
	}
	bodyDrained = true
	if err := publishStored(tmpPath, path, req.BodySize); err != nil {
		return response{}, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return response{}, fmt.Errorf("stat stored file: %w", err)
	}
	return st.recordPut(req, actionHex, outputHex, path, allocatedBytes(info))
}

// recordPut upserts the entry for a stored file. The upsert protects an output
// that the entry displaces (see protectDisplaced) before it commits.
func (st *store) recordPut(req request, actionHex, outputHex, path string, storedSize int64) (response, error) {
	now := time.Now()
	ent := entry{
		ActionID:       actionHex,
		OutputID:       outputHex,
		Size:           req.BodySize,
		CompressedSize: storedSize,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.upsertEntry(ent); err != nil {
		return response{}, err
	}
	return response{ID: req.ID, DiskPath: path}, nil
}

// publishStored moves the temp file to its final path. link gives first-writer-
// wins. When another writer published first, its file stays if its size
// matches; a file of the wrong size is damaged and the temp file replaces it.
func publishStored(tmpPath, path string, size int64) error {
	err := os.Link(tmpPath, path)
	switch {
	case err == nil:
	case errors.Is(err, fs.ErrExist):
		info, statErr := os.Lstat(path)
		if statErr == nil && info.Mode().IsRegular() && info.Size() == size {
			break
		}
		if renameErr := os.Rename(tmpPath, path); renameErr != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("replace damaged stored file: %w", renameErr)
		}
		return nil
	default:
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publish stored file: %w", err)
	}
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove temp file: %w", err)
	}
	return nil
}

// get returns the stored path of the entry. It does not stat or read the file:
// cmd/go checks the path itself and treats a missing or wrong-sized file as a
// miss.
func (st *store) get(req request) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	ent, found, err := st.lookupEntry(actionHex)
	if !found && err == nil {
		return response{ID: req.ID, Miss: true}, nil
	}
	if err != nil {
		return response{}, err
	}

	outputID, err := hex.DecodeString(ent.OutputID)
	if err != nil {
		return response{}, fmt.Errorf("decode output ID: %w", err)
	}
	st.markEntryAccess(actionHex)
	return response{
		ID:       req.ID,
		OutputID: outputID,
		Size:     ent.Size,
		Time:     &ent.CreatedAt,
		DiskPath: st.storedPath(ent.OutputID),
	}, nil
}

func (st *store) markEntryAccess(actionID string) {
	if err := st.markEntryAccessAt(actionID, time.Now()); err != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", err)
	}
}

func (st *store) markEntryAccessAt(actionID string, now time.Time) error {
	st.mu.Lock()
	st.accessed[actionID] = unixMillis(now)
	elapsed := now.Sub(st.lastAccessFlush)
	shouldFlush := elapsed >= accessFlushInterval ||
		(len(st.accessed) >= accessFlushThreshold && elapsed >= accessFlushMinInterval)
	if !shouldFlush {
		st.mu.Unlock()
		return nil
	}
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.lastAccessFlush = now
	st.mu.Unlock()
	return st.writeAccessTimes(accessed)
}

func (st *store) flushAccessTimes() error {
	return st.flushAccessTimesAt(time.Now())
}

func (st *store) flushAccessTimesAt(now time.Time) error {
	st.mu.Lock()
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.lastAccessFlush = now
	st.mu.Unlock()
	if len(accessed) == 0 {
		return nil
	}
	return st.writeAccessTimes(accessed)
}

func (st *store) writeAccessTimes(accessed map[string]int64) error {
	err := st.db.withTx(context.Background(), func(conn *sqlite.Conn) error {
		return touchEntries(conn, accessed)
	})
	if err == nil {
		return nil
	}

	st.mu.Lock()
	for actionID, accessedAt := range accessed {
		st.accessed[actionID] = max(st.accessed[actionID], accessedAt)
	}
	st.mu.Unlock()
	return err
}

func (st *store) upsertEntry(ent entry) error {
	if err := st.q.upsertEntry(context.Background(), ent, st.protectDisplaced); err != nil {
		return fmt.Errorf("upsert entry: %w", err)
	}
	return nil
}

// protectDisplaced starts the floor for an output that an upsert is about to
// leave without a row: it sets the stored file's times to now, because a run
// that read the old row may still hold the path. A missing file needs no
// protection. Any other error fails the upsert, so the old row stays.
func (st *store) protectDisplaced(outputHex string) error {
	if !validOutputHex(outputHex) {
		return nil
	}
	now := time.Now()
	if err := st.chtimes(st.storedPath(outputHex), now, now); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("protect displaced output %s: %w", outputHex, err)
	}
	return nil
}

func (st *store) lookupEntry(actionID string) (entry, bool, error) {
	return st.q.lookupEntry(context.Background(), actionID)
}

// prune runs every full-prune step now, if no run is active.
func (st *store) prune() error {
	return st.withLifecycleLock(func() error {
		activeRuns, err := st.prepareToPruneLocked()
		if err != nil || activeRuns > 0 {
			return err
		}
		return st.fullPruneLocked(time.Now(), st.maxSize)
	})
}

// pruneAutomatically runs at close. Nothing is deleted while any run is
// active: a run may hold a path that get or put returned. With no active run,
// the size prune runs every time and the full prune runs once per hour.
func (st *store) pruneAutomatically(now time.Time) error {
	return st.withLifecycleLock(func() error {
		activeRuns, err := st.prepareToPruneLocked()
		if err != nil || activeRuns > 0 {
			return err
		}

		fullPruneDue, err := st.fullPruneDue(now)
		if err != nil {
			return err
		}
		if !fullPruneDue {
			return st.pruneSizeWithHysteresis(now)
		}
		if err := st.fullPruneLocked(now, st.maxSize*sizePruneTargetPercent/100); err != nil {
			return err
		}
		return st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now))
	})
}

// fullPruneLocked ages out old entries, walks the outputs folder, and prunes by
// size. The caller holds the lifecycle lock and has seen no active run.
func (st *store) fullPruneLocked(now time.Time, target int64) error {
	if err := st.pruneOldDataLocked(now); err != nil {
		return err
	}
	if err := st.walkOutputs(now); err != nil {
		return err
	}
	return st.pruneSizeLocked(target, now)
}

func (st *store) pruneSizeWithHysteresis(now time.Time) error {
	if st.maxSize <= 0 {
		return nil
	}
	target := st.maxSize * sizePruneTargetPercent / 100
	return st.pruneSizeLocked(target, now)
}

func (st *store) prepareToPruneLocked() (int64, error) {
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	activeRuns, err := st.q.countRuns(context.Background())
	if err != nil {
		return 0, fmt.Errorf("count active runs: %w", err)
	}
	return activeRuns, nil
}

func (st *store) pruneOldDataLocked(now time.Time) error {
	// Age-based entry deletion reconciles size in the same transaction.
	if st.maxAge > 0 {
		return st.pruneOldEntries(now)
	}
	if _, err := st.q.reconcileCompressedSize(context.Background()); err != nil {
		return fmt.Errorf("reconcile compressed size: %w", err)
	}
	return nil
}

// pruneSizeLocked deletes outputs, oldest last use first, once the counter is
// above maxSize. It stops at target, and it never deletes an output that was
// used less than useFloor ago.
func (st *store) pruneSizeLocked(target int64, now time.Time) error {
	if st.maxSize <= 0 {
		return nil
	}
	total, err := st.compressedSize()
	if err != nil {
		return err
	}
	if total <= st.maxSize {
		return nil
	}

	candidates, err := st.pruneCandidates(now)
	if err != nil {
		return err
	}
	removed := 0
	for _, candidate := range candidates {
		if total <= target {
			break
		}
		if err := st.removeStored(candidate.outputID); err != nil {
			return err
		}
		if err := st.q.deleteEntriesByOutputID(context.Background(), candidate.outputID); err != nil {
			return fmt.Errorf("delete pruned entries: %w", err)
		}
		total -= candidate.size
		removed++
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d outputs, stored size now %s", removed, formatSize(total))
	}
	return nil
}

// removeStored deletes the stored file of an output. A missing file is fine.
// An output ID that is not lowercase hex names no stored file.
func (st *store) removeStored(outputHex string) error {
	if !validOutputHex(outputHex) {
		return nil
	}
	if err := os.Remove(st.storedPath(outputHex)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stored file: %w", err)
	}
	return nil
}

func (st *store) fullPruneDue(now time.Time) (bool, error) {
	lastMillis, found, err := st.q.fullPruneState(context.Background())
	if err != nil {
		return false, fmt.Errorf("read last full prune: %w", err)
	}
	if !found {
		return true, nil
	}
	last := millisTime(lastMillis)
	if last.After(now) {
		return true, nil
	}
	return now.Sub(last) >= fullPruneInterval, nil
}

type pruneCandidate struct {
	outputID string
	size     int64
}

func (st *store) compressedSize() (int64, error) {
	total, err := st.q.compressedSize(context.Background())
	if err != nil {
		return 0, fmt.Errorf("calculate compressed size: %w", err)
	}
	return total, nil
}

func (st *store) pruneCandidates(now time.Time) ([]pruneCandidate, error) {
	candidates, err := st.q.pruneCandidates(context.Background(), unixMillis(now.Add(-useFloor)))
	if err != nil {
		return nil, fmt.Errorf("query prune candidates: %w", err)
	}
	return candidates, nil
}

func (st *store) pruneOldEntries(now time.Time) error {
	if st.maxAge <= 0 {
		return nil
	}
	cutoff := unixMillis(trimCutoff(st.maxAge, now))
	removed, err := st.q.deleteEntriesAccessedBefore(context.Background(), cutoff)
	if err != nil {
		return fmt.Errorf("prune old entries: %w", err)
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d entries not used in %s", removed, st.maxAge)
	}
	return nil
}

func trimCutoff(maxAge time.Duration, now time.Time) time.Time {
	return now.Add(-maxAge - mtimeInterval)
}

// walkOutputs removes the files in the outputs folder that no row needs, then
// sets the counter to the bytes on disk under the version folder. The caller
// holds the lifecycle lock and has seen no active run, so every temp file
// belongs to a dead writer. A file with no row stays until its mtime is older
// than useFloor: a displaced output has no row but may still be in use.
func (st *store) walkOutputs(now time.Time) error {
	// Output files are already partitioned by the first byte of their hex ID.
	// Reconcile one shard at a time to keep memory bounded without per-file SQL.
	cutoff := now.Add(-useFloor)
	referenced := make(map[string]struct{})
	for shard := range 256 {
		lower := fmt.Sprintf("%02x", shard)
		upper := fmt.Sprintf("%02x", shard+1)
		if shard == 255 {
			upper = "g"
		}
		if err := st.q.referencedOutputIDs(context.Background(), lower, upper, referenced); err != nil {
			return fmt.Errorf("query referenced outputs in shard %s: %w", lower, err)
		}
		if err := removeUnneededOutputFiles(filepath.Join(st.outputsDir, lower), referenced, cutoff); err != nil {
			return fmt.Errorf("remove unneeded outputs in shard %s: %w", lower, err)
		}
	}
	total, err := storedBytesUnder(st.versionDir)
	if err != nil {
		return fmt.Errorf("measure stored size: %w", err)
	}
	if err := st.q.setState(context.Background(), compressedSizeStateKey, total); err != nil {
		return fmt.Errorf("set stored size: %w", err)
	}
	return nil
}

// removeUnneededOutputFiles removes every temp file in dir, and every file that
// is not in referenced and was last written before cutoff.
func removeUnneededOutputFiles(dir string, referenced map[string]struct{}, cutoff time.Time) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.Contains(name, tempMarker) {
			if _, ok := referenced[name]; ok {
				continue
			}
			info, err := entry.Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.ModTime().Before(cutoff) {
				continue
			}
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	removeDirIfEmpty(dir)
	return nil
}

// storedBytesUnder sums the bytes on disk (st_blocks * 512) of every regular
// file under root.
func storedBytesUnder(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		total += allocatedBytes(info)
		return nil
	})
	return total, err
}

// allocatedBytes returns the bytes a file occupies on disk. A file compressed
// by the file system reports its compressed size; one that is compressed inline
// reports 0.
func allocatedBytes(info fs.FileInfo) int64 {
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		return sys.Blocks * 512
	}
	return info.Size()
}

func removeDirIfEmpty(path string) {
	_ = os.Remove(path)
}

// validOutputHex reports whether id can name a stored file: lowercase hex with
// at least the two characters of its shard.
func validOutputHex(id string) bool {
	if len(id) < 2 {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (st *store) storedDir(outputHex string) string {
	return storedDir(st.outputsDir, outputHex)
}

func storedDir(outputsDir, outputHex string) string {
	shard := "xx"
	if len(outputHex) >= 2 {
		shard = outputHex[:2]
	}
	return filepath.Join(outputsDir, shard)
}

func (st *store) storedPath(outputHex string) string {
	return storedPath(st.outputsDir, outputHex)
}

func storedPath(outputsDir, outputHex string) string {
	return filepath.Join(storedDir(outputsDir, outputHex), outputHex)
}
