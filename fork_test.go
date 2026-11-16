package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Expected values in these tests come from the fork's design: stored files at
// <versionDir>/outputs/<first two hex chars>/<outputID hex>, compressed in the
// file system, never deleted while a run is active or inside the 1 hour floor.

const forkResourceForkBodySize = 300_000

// forkBody returns a body of forkResourceForkBodySize pseudo-random bytes. The
// stored form is the resource-fork form (4 full chunks plus 37,856 bytes), so
// its row has CompressedSize > 0. Each seed gives a different output.
func forkBody(seed byte) []byte {
	var key [32]byte
	key[0] = seed
	body := make([]byte, forkResourceForkBodySize)
	if _, err := rand.NewChaCha8(key).Read(body); err != nil {
		panic(err)
	}
	return body
}

func newForkStore(t *testing.T, cfg config) *store {
	t.Helper()

	if cfg.dir == "" {
		cfg.dir = t.TempDir()
	}
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func forkOutputPath(st *store, outputID []byte) string {
	outputHex := hexOf(outputID)
	return filepath.Join(st.versionDir, "outputs", outputHex[:2], outputHex)
}

func forkPutRequest(id int64, actionID, outputID []byte, size int) request {
	return request{ID: id, Command: cmdPut, ActionID: actionID, OutputID: outputID, BodySize: int64(size)}
}

func forkBodyInput(body []byte) io.Reader {
	if len(body) == 0 {
		return strings.NewReader("")
	}
	return encodedBody(body)
}

func forkPut(t *testing.T, st *store, actionID, outputID, body []byte) response {
	t.Helper()

	res, err := st.put(forkPutRequest(1, actionID, outputID, len(body)), bufio.NewReader(forkBodyInput(body)))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func forkLstat(t *testing.T, path string) unix.Stat_t {
	t.Helper()

	var info unix.Stat_t
	if err := unix.Lstat(path, &info); err != nil {
		t.Fatalf("lstat %s: %v", path, err)
	}
	return info
}

// forkTryLstat returns a zero Stat_t when the path cannot be read.
func forkTryLstat(path string) unix.Stat_t {
	var info unix.Stat_t
	if err := unix.Lstat(path, &info); err != nil {
		return unix.Stat_t{}
	}
	return info
}

func forkStoredBytes(info unix.Stat_t) int64 {
	return info.Blocks * 512
}

// forkFiles lists the regular files under root, relative and sorted, for which
// keep returns true.
func forkFiles(t *testing.T, root string, keep func(rel string) bool) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if keep(rel) {
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(files)
	return files
}

func forkKeepAll(string) bool { return true }

// forkKeepData drops the catalog, the lifecycle lock, and the run folders.
func forkKeepData(rel string) bool {
	base := filepath.Base(rel)
	return !strings.HasPrefix(base, "cache.db") && base != "lifecycle.lock" && base != "run.lock"
}

func forkEntry(t *testing.T, st *store, actionID []byte) entry {
	t.Helper()

	ent, found, err := st.lookupEntry(hexOf(actionID))
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("no catalog row for action %x", actionID)
	}
	return ent
}

func forkHasEntry(t *testing.T, st *store, actionID []byte) bool {
	t.Helper()

	_, found, err := st.lookupEntry(hexOf(actionID))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// forkGoIdle deletes the store's own run row, as close does, so that
// pruneAutomatically sees no active run.
func forkGoIdle(t *testing.T, st *store) {
	t.Helper()

	if err := st.q.deleteRun(context.Background(), st.runID); err != nil {
		t.Fatal(err)
	}
}

func forkSetAccessedAt(t *testing.T, st *store, actionID []byte, at time.Time) {
	t.Helper()

	execDB(t, st.db, `UPDATE entries SET accessed_at = ? WHERE action_id = ?`, unixMillis(at), hexOf(actionID))
}

func forkRequireNoTemp(t *testing.T, st *store) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(st.versionDir, "outputs", "*", "*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Errorf("temp files left behind: %v", matches)
	}
}

func forkRequireNextRequest(t *testing.T, br *bufio.Reader, wantID int64) {
	t.Helper()

	next, err := readRequest(br)
	if err != nil || next.ID != wantID {
		t.Fatalf("body was not consumed: next=%+v, err=%v, want request %d", next, err, wantID)
	}
}

const forkCloseRequest = "{\"ID\":99,\"Command\":\"close\"}\n"

func TestForkPutStoresCompressedFile(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"inline", bytes.Repeat([]byte("inline text "), 334)[:4000]},
		{"resource-fork", forkBody(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newForkStore(t, config{})
			defer st.close()
			actionID := sha256Sum([]byte("action " + tc.name))
			outputID := sha256Sum(tc.body)
			res := forkPut(t, st, actionID, outputID, tc.body)

			want := forkOutputPath(st, outputID)
			if res.DiskPath != want {
				t.Fatalf("DiskPath = %q, want %q", res.DiskPath, want)
			}
			info := forkLstat(t, want)
			if info.Mode&unix.S_IFMT != unix.S_IFREG {
				t.Errorf("stored file mode = %#o, want a regular file", info.Mode)
			}
			if info.Flags&unix.UF_COMPRESSED == 0 {
				t.Errorf("stored file flags = %#x, want UF_COMPRESSED", info.Flags)
			}
			if info.Size != int64(len(tc.body)) {
				t.Errorf("stored file st_size = %d, want %d", info.Size, len(tc.body))
			}
			if perm := info.Mode & 0o777; perm != 0o444 {
				t.Errorf("stored file permissions = %#o, want 0444", perm)
			}
			got, err := os.ReadFile(want)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Error("stored file bytes differ from the body")
			}
			forkRequireNoTemp(t, st)
			if ent := forkEntry(t, st, actionID); ent.CompressedSize != forkStoredBytes(info) {
				t.Errorf("CompressedSize = %d, want st_blocks*512 = %d", ent.CompressedSize, forkStoredBytes(info))
			}
			if tc.name == "resource-fork" && forkStoredBytes(info) == 0 {
				t.Error("resource-fork body stored 0 bytes on disk")
			}
			if filepath.Base(st.versionDir) != "v2" {
				t.Errorf("versionDir = %q, want it to end in v2", st.versionDir)
			}
		})
	}
}

func TestForkWarmGetReturnsStoredPathUnchanged(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir()}
	body := forkBody(2)
	actionID := sha256Sum([]byte("warm action"))
	outputID := sha256Sum(body)

	a := newForkStore(t, cfg)
	put := forkPut(t, a, actionID, outputID, body)
	a.close()
	if want := forkOutputPath(a, outputID); put.DiskPath != want {
		t.Errorf("put DiskPath = %q, want the stored path %q", put.DiskPath, want)
	}
	before := forkTryLstat(put.DiskPath)

	b := newForkStore(t, cfg)
	defer b.close()
	filesBefore := forkFiles(t, b.versionDir, forkKeepData)
	res, err := b.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Miss || res.DiskPath != put.DiskPath {
		t.Errorf("get = %+v, want a hit at the put path %q", res, put.DiskPath)
	}
	after := forkTryLstat(put.DiskPath)
	if after.Ino != before.Ino || after.Blocks != before.Blocks {
		t.Errorf("stored file changed: inode %d -> %d, blocks %d -> %d", before.Ino, after.Ino, before.Blocks, after.Blocks)
	}
	if filesAfter := forkFiles(t, b.versionDir, forkKeepData); !slices.Equal(filesBefore, filesAfter) {
		t.Errorf("files changed by get:\nbefore %v\nafter  %v", filesBefore, filesAfter)
	}
}

func TestForkConcurrentPutPublishesOneFile(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir()}
	stores := []*store{newForkStore(t, cfg), newForkStore(t, cfg)}
	defer stores[0].close()
	defer stores[1].close()
	body := forkBody(3)
	outputID := sha256Sum(body)

	type result struct {
		res response
		err error
	}
	results := make([]result, len(stores))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, st := range stores {
		wg.Go(func() {
			<-start
			actionID := sha256Sum([]byte(fmt.Sprintf("concurrent action %d", i)))
			res, err := st.put(forkPutRequest(1, actionID, outputID, len(body)), bufio.NewReader(encodedBody(body)))
			results[i] = result{res, err}
		})
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Fatalf("store %d put: %v", i, r.err)
		}
	}
	if results[0].res.DiskPath != results[1].res.DiskPath {
		t.Errorf("paths differ: %q and %q", results[0].res.DiskPath, results[1].res.DiskPath)
	}
	want := forkOutputPath(stores[0], outputID)
	shard := forkFiles(t, filepath.Dir(want), forkKeepAll)
	if len(shard) != 1 || shard[0] != filepath.Base(want) {
		t.Errorf("shard holds %v, want exactly %s", shard, filepath.Base(want))
	}
	got, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Error("stored file bytes differ from the body")
	}
}

func TestForkPutFailsClosedWhenFlagFails(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	st.setCompressedFlag = func(int) error { return unix.EPERM }
	body := forkBody(4)
	actionID := sha256Sum([]byte("flag action"))
	outputID := sha256Sum(body)

	br := bufio.NewReader(io.MultiReader(encodedBody(body), strings.NewReader(forkCloseRequest)))
	if _, err := st.put(forkPutRequest(1, actionID, outputID, len(body)), br); err == nil {
		t.Error("put succeeded although the flag step failed")
	}
	if files := forkFiles(t, filepath.Join(st.versionDir, "outputs"), forkKeepAll); len(files) != 0 {
		t.Errorf("files left in outputs: %v", files)
	}
	forkRequireNoTemp(t, st)
	if forkHasEntry(t, st, actionID) {
		t.Error("row registered for the failed put")
	}
	forkRequireNextRequest(t, br, 99)
}

func TestForkPutRejectsBodyOf2GiB(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	actionID := sha256Sum([]byte("2 GiB action"))
	outputID := sha256Sum([]byte("2 GiB output"))

	br := bufio.NewReader(strings.NewReader("\"AAAA\"\n" + forkCloseRequest))
	_, err := st.put(forkPutRequest(1, actionID, outputID, 1<<31), br)
	if err == nil || !strings.Contains(err.Error(), "2 GiB") {
		t.Errorf("put error = %v, want one that names the 2 GiB limit", err)
	}
	forkRequireNextRequest(t, br, 99)
	if forkHasEntry(t, st, actionID) {
		t.Error("row registered for the rejected put")
	}
	if files := forkFiles(t, filepath.Join(st.versionDir, "outputs"), forkKeepAll); len(files) != 0 {
		t.Errorf("files left in outputs: %v", files)
	}
}

func TestForkNoDeletionWithActiveRun(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir(), maxSize: 1}
	a := newForkStore(t, cfg)
	b := newForkStore(t, cfg)
	defer b.close()

	actions := make([][]byte, 0, 3)
	outputs := make([][]byte, 0, 3)
	paths := make([]string, 0, 3)
	for i := range 3 {
		body := forkBody(byte(10 + i))
		actions = append(actions, sha256Sum([]byte(fmt.Sprintf("held action %d", i))))
		outputs = append(outputs, sha256Sum(body))
		paths = append(paths, forkPut(t, a, actions[i], outputs[i], body).DiskPath)
	}
	res, err := b.get(request{ID: 2, Command: cmdGet, ActionID: actions[0]})
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, actionID := range actions {
		forkSetAccessedAt(t, a, actionID, old)
		if ent := forkEntry(t, a, actionID); ent.CompressedSize <= 0 {
			t.Fatalf("row for %x has CompressedSize %d, want > 0", actionID, ent.CompressedSize)
		}
	}
	if counter := compressedSizeForTest(t, a); counter <= a.maxSize {
		t.Fatalf("counter = %d, want above maxSize %d", counter, a.maxSize)
	}
	keep := func(rel string) bool {
		return forkKeepData(rel) && !strings.HasPrefix(rel, "live"+string(filepath.Separator))
	}
	before := forkFiles(t, a.versionDir, keep)

	a.close()

	if after := forkFiles(t, a.versionDir, keep); !slices.Equal(before, after) {
		t.Errorf("files deleted while a run was active:\nbefore %v\nafter  %v", before, after)
	}
	if res.Miss || res.DiskPath != paths[0] {
		t.Fatalf("get = %+v, want a hit at the put path %q", res, paths[0])
	}
	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, forkBody(10)) {
		t.Error("shared output differs from the body after the first run closed")
	}
}

func TestForkIdlePruneKeepsFloorAndTrims(t *testing.T) {
	t.Parallel()

	type output struct {
		actionID, outputID []byte
		age                time.Duration
		size               int64
	}
	setup := func(t *testing.T, ages []time.Duration, now time.Time) (*store, []output, int64) {
		t.Helper()

		st := newForkStore(t, config{})
		outs := make([]output, 0, len(ages))
		var total int64
		for i, age := range ages {
			body := forkBody(byte(20 + i))
			out := output{
				actionID: sha256Sum([]byte(fmt.Sprintf("prune action %d", i))),
				outputID: sha256Sum(body),
				age:      age,
			}
			forkPut(t, st, out.actionID, out.outputID, body)
			out.size = forkEntry(t, st, out.actionID).CompressedSize
			if out.size <= 0 {
				t.Fatalf("output %d has CompressedSize %d, want > 0", i, out.size)
			}
			forkSetAccessedAt(t, st, out.actionID, now.Add(-age))
			total += out.size
			outs = append(outs, out)
		}
		if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now)); err != nil {
			t.Fatal(err)
		}
		forkGoIdle(t, st)
		return st, outs, total
	}
	exists := func(st *store, out output) (bool, bool) {
		_, err := os.Lstat(forkOutputPath(st, out.outputID))
		_, found, lookupErr := st.lookupEntry(hexOf(out.actionID))
		if lookupErr != nil {
			panic(lookupErr)
		}
		return err == nil, found
	}

	t.Run("old-outputs-suffice", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		st, outs, total := setup(t, []time.Duration{3 * time.Hour, 5 * time.Hour, 2 * time.Hour, 4 * time.Hour}, now)
		defer st.close()
		st.maxSize = total * 3 / 4
		if counter := compressedSizeForTest(t, st); counter != total || counter <= st.maxSize {
			t.Fatalf("counter = %d, want %d above maxSize %d", counter, total, st.maxSize)
		}

		if err := st.pruneAutomatically(now); err != nil {
			t.Fatal(err)
		}

		order := slices.Clone(outs)
		slices.SortFunc(order, func(x, y output) int { return int(y.age - x.age) })
		wantRemoved := map[string]bool{}
		remaining := total
		for _, out := range order {
			if remaining <= st.maxSize*90/100 {
				break
			}
			wantRemoved[hexOf(out.outputID)] = true
			remaining -= out.size
		}
		if len(wantRemoved) == 0 || len(wantRemoved) == len(outs) {
			t.Fatalf("fixture removes %d of %d outputs, want some but not all", len(wantRemoved), len(outs))
		}
		for _, out := range outs {
			file, row := exists(st, out)
			if removed := wantRemoved[hexOf(out.outputID)]; file == removed || row == removed {
				t.Errorf("output %x (age %v): file=%t row=%t, want both %t", out.outputID[:4], out.age, file, row, !removed)
			}
		}
		if counter := compressedSizeForTest(t, st); counter != remaining {
			t.Errorf("counter = %d, want %d", counter, remaining)
		}
	})

	t.Run("floor-blocks", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		st, outs, total := setup(t, []time.Duration{2 * time.Hour, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute}, now)
		defer st.close()
		st.maxSize = total / 4
		if counter := compressedSizeForTest(t, st); counter <= st.maxSize {
			t.Fatalf("counter = %d, want above maxSize %d", counter, st.maxSize)
		}

		if err := st.pruneAutomatically(now); err != nil {
			t.Fatal(err)
		}

		for i, out := range outs {
			file, row := exists(st, out)
			if wantKept := i != 0; file != wantKept || row != wantKept {
				t.Errorf("output %d (age %v): file=%t row=%t, want both %t", i, out.age, file, row, wantKept)
			}
		}
		if counter := compressedSizeForTest(t, st); counter <= st.maxSize {
			t.Errorf("counter = %d, want it still above maxSize %d (the floor protects the rest)", counter, st.maxSize)
		}
	})
}

func TestForkWalkSetsCounterToTotal(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	bodies := [][]byte{forkBody(30), forkBody(31), nil}
	paths := make([]string, 0, len(bodies))
	var sumRows int64
	for i, body := range bodies {
		actionID := sha256Sum([]byte(fmt.Sprintf("walk action %d", i)))
		outputID := sha256Sum(body)
		res := forkPut(t, st, actionID, outputID, body)
		if want := forkOutputPath(st, outputID); res.DiskPath != want {
			t.Errorf("put DiskPath = %q, want the stored path %q", res.DiskPath, want)
		}
		paths = append(paths, res.DiskPath)
		sumRows += forkEntry(t, st, actionID).CompressedSize
	}

	// A tool that decompresses a stored file in place: chmod, open for write, chmod back.
	path := paths[0]
	rowSize := forkStoredBytes(forkLstat(t, path))
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	// Random data compresses to more bytes than it has, so the plain file is
	// smaller here. Either way the row no longer matches the disk.
	if changed := forkStoredBytes(forkLstat(t, path)); changed == rowSize {
		t.Errorf("in-place decompression did not change the file on disk: %d bytes", changed)
	}

	err = st.withLifecycleLock(func() error {
		var sum int64
		for _, rel := range forkFiles(t, st.versionDir, forkKeepAll) {
			sum += forkStoredBytes(forkLstat(t, filepath.Join(st.versionDir, rel)))
		}
		if err := st.walkOutputs(time.Now()); err != nil {
			return err
		}
		if counter := compressedSizeForTest(t, st); counter != sum {
			t.Errorf("counter = %d, want the st_blocks total %d", counter, sum)
		} else if counter <= sumRows {
			t.Errorf("counter = %d, want more than the %d the rows report", counter, sumRows)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestForkWalkRemovesStaleTempsAndOrphans(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	body := []byte("stored with a row")
	actionID := sha256Sum([]byte("walk row action"))
	outputID := sha256Sum(body)
	forkPut(t, st, actionID, outputID, body)

	stored := forkOutputPath(st, outputID)
	shard := filepath.Dir(stored)
	prefix := filepath.Base(shard)
	if err := os.MkdirAll(shard, 0o777); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	write := func(name string, age time.Duration) string {
		path := filepath.Join(shard, name)
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		at := now.Add(-age)
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
		return path
	}
	temp := write(prefix+strings.Repeat("11", 31)+".tmp-1", 0)
	oldOrphan := write(prefix+strings.Repeat("22", 31), 2*time.Hour)
	newOrphan := write(prefix+strings.Repeat("33", 31), 10*time.Minute)
	forkGoIdle(t, st)

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		path string
		want bool
	}{
		{"temp file", temp, false},
		{"orphan older than the floor", oldOrphan, false},
		{"orphan inside the floor", newOrphan, true},
		{"stored file with a row", stored, true},
	} {
		if _, err := os.Lstat(tc.path); (err == nil) != tc.want {
			t.Errorf("%s: lstat err = %v, want present=%t", tc.name, err, tc.want)
		}
	}
}

func TestForkDisplacedOutputKeepsFileForFloor(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	body1, body2 := forkBody(40), forkBody(41)
	outputID1, outputID2 := sha256Sum(body1), sha256Sum(body2)
	actionID := sha256Sum([]byte("displacement action"))

	res1 := forkPut(t, st, actionID, outputID1, body1)
	path1 := res1.DiskPath
	if want := forkOutputPath(st, outputID1); path1 != want {
		t.Errorf("put DiskPath = %q, want the stored path %q", path1, want)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path1, old, old); err != nil {
		t.Fatal(err)
	}
	if res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil || res.Miss {
		t.Fatalf("get = %+v, err=%v", res, err)
	}
	res2 := forkPut(t, st, actionID, outputID2, body2)
	path2 := res2.DiskPath

	info, err := os.Lstat(path1)
	if err != nil {
		t.Fatalf("displaced output: %v", err)
	}
	mtime := info.ModTime()
	if d := time.Since(mtime); d < 0 || d > 5*time.Second {
		t.Fatalf("displaced output mtime is %v ago, want within 5s of now", d)
	}
	forkGoIdle(t, st)
	execDB(t, st.db, `DELETE FROM state WHERE key = ?`, lastFullPruneStateKey)

	if err := st.pruneAutomatically(mtime.Add(30 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path1); err != nil {
		t.Fatalf("displaced output is gone 30 minutes after displacement: %v", err)
	}
	if err := st.pruneAutomatically(mtime.Add(91 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path1); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("displaced output lstat err = %v, want not exist after the floor", err)
	}
	if _, err := os.Lstat(path2); err != nil {
		t.Errorf("current output removed: %v", err)
	}
}

func TestForkSharedOutputKeepsFloorWhenOneActionIsReplaced(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	body1, body2 := forkBody(70), forkBody(71)
	outputID1, outputID2 := sha256Sum(body1), sha256Sum(body2)
	actionA, actionB := sha256Sum([]byte("shared action A")), sha256Sum([]byte("shared action B"))

	path1 := forkPut(t, st, actionA, outputID1, body1).DiskPath
	forkPut(t, st, actionB, outputID1, body1)
	old := time.Now().Add(-3 * time.Hour)
	forkSetAccessedAt(t, st, actionA, old)
	forkSetAccessedAt(t, st, actionB, old)
	if err := os.Chtimes(path1, old, old); err != nil {
		t.Fatal(err)
	}
	// A tool takes the shared path through A now; the access is not flushed yet.
	if res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionA}); err != nil || res.Miss || res.DiskPath != path1 {
		t.Fatalf("get A = %+v, err=%v", res, err)
	}
	// A moves to another output; B still names the shared one.
	path2 := forkPut(t, st, actionA, outputID2, body2).DiskPath

	forkGoIdle(t, st)
	now := time.Now()
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now)); err != nil {
		t.Fatal(err)
	}
	// The counter holds both outputs, so it is above the maximum.
	st.maxSize = forkEntry(t, st, actionA).CompressedSize
	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path1); err != nil {
		t.Errorf("shared output was handed out through A moments ago, but the idle prune removed it: %v", err)
	}
	if !forkHasEntry(t, st, actionB) {
		t.Error("the row of B was pruned inside the floor of the shared output")
	}
	if _, err := os.Lstat(path2); err != nil {
		t.Errorf("current output of A removed: %v", err)
	}
}

func TestForkDisplacementFailsPutWhenChtimesFails(t *testing.T) {
	t.Parallel()

	st := newForkStore(t, config{})
	defer st.close()
	body1, body2 := forkBody(50), forkBody(51)
	outputID1, outputID2 := sha256Sum(body1), sha256Sum(body2)
	actionID := sha256Sum([]byte("rollback action"))
	forkPut(t, st, actionID, outputID1, body1)
	counter := compressedSizeForTest(t, st)

	st.chtimes = func(string, time.Time, time.Time) error { return unix.EPERM }
	br := bufio.NewReader(io.MultiReader(encodedBody(body2), strings.NewReader(forkCloseRequest)))
	if _, err := st.put(forkPutRequest(2, actionID, outputID2, len(body2)), br); err == nil {
		t.Error("put succeeded although the displaced file could not be protected")
	}
	if ent := forkEntry(t, st, actionID); ent.OutputID != hexOf(outputID1) {
		t.Errorf("row names output %s, want the old output %x", ent.OutputID, outputID1)
	}
	if _, err := os.Lstat(forkOutputPath(st, outputID1)); err != nil {
		t.Errorf("old output missing: %v", err)
	}
	if got := compressedSizeForTest(t, st); got != counter {
		t.Errorf("counter = %d, want %d", got, counter)
	}
	forkRequireNextRequest(t, br, 99)
}

func TestForkIdlePruneSkippedAfterFailedFlush(t *testing.T) {
	// The log package is global: this test runs alone, without t.Parallel.
	for _, tc := range []struct {
		name      string
		flushErr  error
		wantKept  bool
		wantInLog string
	}{
		{"flush-fails", errors.New("injected flush failure"), true, "injected flush failure"},
		{"flush-ok", nil, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			log.SetOutput(&logs)
			defer log.SetOutput(os.Stderr)

			st := newForkStore(t, config{maxSize: 1, verbose: true})
			dbPath := filepath.Join(st.versionDir, "cache.db")
			body := forkBody(60)
			actionID := sha256Sum([]byte("flush action"))
			outputID := sha256Sum(body)
			forkPut(t, st, actionID, outputID, body)
			forkSetAccessedAt(t, st, actionID, time.Now().Add(-2*time.Hour))
			if counter := compressedSizeForTest(t, st); counter <= st.maxSize {
				t.Fatalf("counter = %d, want above maxSize %d", counter, st.maxSize)
			}
			if tc.flushErr != nil {
				st.flushAccess = func() error { return tc.flushErr }
			}

			st.close()

			if tc.wantInLog != "" && !strings.Contains(logs.String(), tc.wantInLog) {
				t.Errorf("close did not report the flush error; log = %q", logs.String())
			}
			_, fileErr := os.Lstat(forkOutputPath(st, outputID))
			var rows int
			queryCatalog(t, dbPath, `SELECT COUNT(*) FROM entries WHERE action_id = ?`, []any{hexOf(actionID)}, &rows)
			if kept := fileErr == nil && rows == 1; kept != tc.wantKept {
				t.Errorf("stored file err = %v, rows = %d, want kept=%t", fileErr, rows, tc.wantKept)
			}
		})
	}
}
