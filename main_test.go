package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"golang.org/x/sys/unix"
	"golang.org/x/tools/go/gcexportdata"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestGoCommandCache(t *testing.T) {
	t.Setenv("GOENV", "off")
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOWORK", "off")
	t.Setenv("GOTOOLCHAIN", "local")
	t.Setenv("GOCACHEPROG", "")
	rootCmd := exec.CommandContext(t.Context(), "go", "env", "GOROOT")
	root, err := rootCmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	goBin := filepath.Join(strings.TrimSpace(string(root)), "bin")

	dir := t.TempDir()
	helper := filepath.Join(dir, "gocachez")
	if runtime.GOOS == "windows" {
		helper += ".exe"
	}
	goCommand := filepath.Join(goBin, "go")
	build := exec.CommandContext(t.Context(), goCommand, "build", "-o", helper, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build gocachez: %v\n%s", err, output)
	}

	setUserDirEnv(t)
	cacheDir := filepath.Join(dir, "cache")
	t.Setenv("GOCACHE", filepath.Join(dir, "go-build"))
	t.Setenv("GOCACHEPROG", fmt.Sprintf(`"%s" -dir "%s" -max-size 0 -max-age 0`, helper, cacheDir))
	t.Setenv("GOCACHEZ_DIR", "")
	t.Setenv("GOCACHEZ_MAX_SIZE", "")
	t.Setenv("GOCACHEZ_MAX_AGE", "")
	t.Setenv("GOCACHEZ_VERBOSE", "true")

	moduleDir := filepath.Join(dir, "module")
	if err := os.MkdirAll(moduleDir, 0o777); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"go.mod": "module example.com/cachetest\n\ngo 1.25.0\n",
		"main.go": `package main
import "fmt"
func answer() int { return 42 }
func main() { fmt.Println(answer()) }
`,
		"main_test.go": `package main
import "testing"
func TestAnswer(t *testing.T) {
	if got := answer(); got != 42 {
		t.Fatalf("answer() = %d", got)
	}
}
`,
	} {
		if err := os.WriteFile(filepath.Join(moduleDir, name), []byte(content), 0o666); err != nil {
			t.Fatal(err)
		}
	}

	for _, name := range []string{"cold", "warm"} {
		t.Run(name, func(t *testing.T) {
			testGoCommandExports(t, goCommand, moduleDir, cacheDir)
			for _, args := range [][]string{{"run", "."}, {"test", "-count=1", "."}} {
				cmd := exec.CommandContext(t.Context(), goCommand, args...)
				cmd.Dir = moduleDir
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				output, err := cmd.Output()
				if err != nil {
					t.Fatalf("go %s: %v\n%s\n%s", strings.Join(args, " "), err, output, &stderr)
				}
				if args[0] == "run" && strings.TrimSpace(string(output)) != "42" {
					t.Fatalf("go run output = %q, want 42", output)
				}
			}
		})
	}
}

func testGoCommandExports(t *testing.T, goCommand, moduleDir, cacheDir string) {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), goCommand, "list", "-test", "-export", "-json", ".")
	cmd.Dir = moduleDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, &stderr)
	}
	// Reported paths must remain readable after go list and its helper exit.
	dec := json.NewDecoder(bytes.NewReader(output))
	foundPackage, foundTestmain := false, false
	for {
		var pkg struct {
			ImportPath string
			Dir        string
			Export     string
			GoFiles    []string
		}
		if err := dec.Decode(&pkg); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if pkg.Export != "" {
			if !strings.HasPrefix(pkg.Export, cacheDir+string(filepath.Separator)) {
				t.Fatalf("export path bypassed gocachez: %s", pkg.Export)
			}
			data, err := os.ReadFile(pkg.Export)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) == 0 {
				t.Fatalf("empty export data for %s", pkg.ImportPath)
			}
			if pkg.ImportPath == "example.com/cachetest" {
				foundPackage = true
			}
		}
		if pkg.ImportPath != "example.com/cachetest.test" {
			continue
		}
		for _, path := range pkg.GoFiles {
			if !filepath.IsAbs(path) {
				path = filepath.Join(pkg.Dir, path)
			}
			if !strings.HasPrefix(path, cacheDir+string(filepath.Separator)) {
				t.Fatalf("test-main path bypassed gocachez: %s", path)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) == 0 {
				t.Fatalf("empty test-main source: %s", path)
			}
			foundTestmain = true
		}
	}
	if !foundPackage || !foundTestmain {
		t.Fatalf("loaded package = %t, retained test main = %t", foundPackage, foundTestmain)
	}
}

func TestStorePutGet(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	body := bytes.NewBufferString("hello from the cache")
	actionID := bytes.Repeat([]byte{1}, 32)
	outputID := bytes.Repeat([]byte{2}, 32)
	req := request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(body.Len()),
	}
	res, err := st.put(req, bufio.NewReader(encodedBody(body.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(res.DiskPath) {
		t.Fatalf("put DiskPath is not absolute: %q", res.DiskPath)
	}
	outputsDir := filepath.Join(st.versionDir, "outputs")
	if rel, err := filepath.Rel(outputsDir, res.DiskPath); err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("put DiskPath = %q, not under outputs dir %q", res.DiskPath, outputsDir)
	}
	gotBody, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("put body = %q", gotBody)
	}

	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss {
		t.Fatal("get missed")
	}
	if !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("OutputID = %x, want %x", getRes.OutputID, outputID)
	}
	if getRes.Size != int64(len(gotBody)) {
		t.Fatalf("Size = %d, want %d", getRes.Size, len(gotBody))
	}
	gotBody, err = os.ReadFile(getRes.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("get body = %q", gotBody)
	}
}

func TestGetMiss(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{33}, 32)
	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.ID != 1 || !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
}

func TestGetRejectsInvalidCatalogOutputID(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := hexOf(bytes.Repeat([]byte{36}, 32))
	outputID := "not-hex"
	if err := st.upsertEntry(entry{
		ActionID:       actionID,
		OutputID:       outputID,
		Size:           4,
		CompressedSize: 4,
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	if res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: bytes.Repeat([]byte{36}, 32)}); err == nil {
		t.Fatalf("get accepted invalid catalog output ID: %+v", res)
	}
}

func TestOutputIDIsOpaque(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{39}, 32)
	outputID := bytes.Repeat([]byte{40}, 32)
	body := []byte("body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Miss || !bytes.Equal(res.OutputID, outputID) {
		t.Fatalf("get response = %+v, want hit with opaque OutputID %x", res, outputID)
	}
	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestPutAcceptsZeroSizeBody(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{35}, 32)
	outputID := sha256Sum(nil)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 0,
	}, bufio.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(st.versionDir, "outputs", hexOf(outputID)[:2], hexOf(outputID))
	if res.DiskPath != want {
		t.Fatalf("put DiskPath = %q, want %q", res.DiskPath, want)
	}
	var info unix.Stat_t
	if err := unix.Lstat(res.DiskPath, &info); err != nil {
		t.Fatal(err)
	}
	if info.Size != 0 || info.Blocks != 0 || info.Flags&unix.UF_COMPRESSED != 0 || info.Mode&0o777 != 0o444 {
		t.Fatalf("empty stored file: size=%d blocks=%d flags=%#x mode=%#o, want an empty plain file with mode 0444",
			info.Size, info.Blocks, info.Flags, info.Mode&0o777)
	}
	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss || getRes.Size != 0 || getRes.DiskPath != want {
		t.Fatalf("get response = %+v, want a hit at %q with size 0", getRes, want)
	}
}

func TestPutReusesExistingBlobAndRecoversFromEviction(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	body := bytes.Repeat([]byte("duplicate output"), 1024)
	outputID := sha256Sum(body)
	original, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{41}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}

	// The stored file disappears while the next put reads its body.
	removeReader := &removeFileReader{
		reader: encodedBody(body),
		path:   original.DiskPath,
	}
	reused, err := st.put(request{
		ID:       2,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{42}, 32),
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(removeReader))
	if err != nil {
		t.Fatal(err)
	}
	if reused.DiskPath != original.DiskPath {
		t.Fatalf("recovered put path = %q, want %q", reused.DiskPath, original.DiskPath)
	}
	if !removeReader.removed {
		t.Fatal("put did not read the body: nothing was removed during the put")
	}
	// The put saw the file, wrote nothing, and the file is gone: the next put
	// must restore it from its own body.
	if _, err := os.Stat(original.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("stored file stat err = %v, want not exist after removal during put", err)
	}
	for i, id := range []byte{43, 44} {
		reused, err = st.put(request{
			ID:       int64(3 + i),
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{id}, 32),
			OutputID: outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body)))
		if err != nil {
			t.Fatal(err)
		}
		if reused.DiskPath != original.DiskPath {
			t.Fatalf("put with missing stored file path = %q, want %q", reused.DiskPath, original.DiskPath)
		}
		got, err := os.ReadFile(original.DiskPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, body) {
			t.Fatal("stored file recreated from the put body differs from the original")
		}
		if i == 0 {
			if err := os.Remove(original.DiskPath); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestPutRecreatesStaleMaterializedOutput(t *testing.T) {
	t.Parallel()

	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			t.Parallel()
			st, err := newStore(config{dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.close()
			body := []byte("body")
			req := request{
				ID:       1,
				Command:  cmdPut,
				ActionID: sha256Sum([]byte("original")),
				OutputID: sha256Sum(body),
				BodySize: int64(len(body)),
			}
			original, err := st.put(req, bufio.NewReader(encodedBody(body)))
			if err != nil {
				t.Fatal(err)
			}
			if missing {
				err = os.Remove(original.DiskPath)
			} else {
				// A tool made the file writable and truncated it.
				if err = os.Chmod(original.DiskPath, 0o644); err == nil {
					err = os.Truncate(original.DiskPath, 0)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			req.ID = 2
			req.ActionID = sha256Sum([]byte("replacement"))
			res, err := st.put(req, bufio.NewReader(encodedBody(body)))
			if err != nil {
				t.Fatal(err)
			}
			if res.DiskPath != original.DiskPath {
				t.Fatalf("put path = %q, want the same stored path %q", res.DiskPath, original.DiskPath)
			}
			got, err := os.ReadFile(res.DiskPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, body) {
				t.Fatal("restored stored file differs from put body")
			}
		})
	}
}

func TestPutReusesMaterializedOutput(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1 << 20} {
		t.Run(fmt.Sprintf("bytes=%d", size), func(t *testing.T) {
			t.Parallel()
			st, err := newStore(config{dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer st.close()
			body := bytes.Repeat([]byte("x"), size)
			outputID := bytes.Repeat([]byte{44}, 32)
			paths := make(map[string]struct{})
			for i := range 8 {
				var input io.Reader = strings.NewReader("")
				if size != 0 {
					input = encodedBody(body)
				}
				br := bufio.NewReader(io.MultiReader(input, strings.NewReader("\n{\"ID\":99,\"Command\":\"close\"}\n")))
				actionID := sha256Sum([]byte(fmt.Sprintf("action %d", i)))
				res, err := st.put(request{
					ID:       int64(i + 1),
					Command:  cmdPut,
					ActionID: actionID,
					OutputID: outputID,
					BodySize: int64(size),
				}, br)
				if err != nil {
					t.Fatal(err)
				}
				paths[res.DiskPath] = struct{}{}
				next, err := readRequest(br)
				if err != nil || next.ID != 99 || next.Command != cmdClose {
					t.Fatalf("put did not consume its body: next=%+v, err=%v", next, err)
				}
				got, err := st.get(request{ID: 100, Command: cmdGet, ActionID: actionID})
				if err != nil || got.Miss || got.DiskPath != res.DiskPath {
					t.Fatalf("get after put = %+v, err=%v", got, err)
				}
			}
			if len(paths) != 1 {
				t.Fatalf("shared output puts created %d stored files, want 1", len(paths))
			}
			files, err := os.ReadDir(st.runDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0].Name() != "run.lock" {
				t.Fatalf("run folder holds %v, want only run.lock", files)
			}
			for path := range paths {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, body) {
					t.Fatal("stored file differs from the put body")
				}
			}
		})
	}
}

func TestPutRejectsInvalidBody(t *testing.T) {
	t.Parallel()

	for _, size := range []int{4, 64<<10 + 17} {
		body := bytes.Repeat([]byte("x"), size)
		for _, mode := range []string{"existing-file", "no-file"} {
			for _, tt := range []struct {
				name  string
				input string
			}{
				{"short", encodedBody(body[:size-1]).String()},
				{"long", encodedBody(append(bytes.Clone(body), 'x')).String()},
				{"invalid-base64", "\"" + base64.StdEncoding.EncodeToString(body) + "%%%\"\n"},
			} {
				t.Run(fmt.Sprintf("bytes=%d/%s/%s", size, mode, tt.name), func(t *testing.T) {
					t.Parallel()
					st, err := newStore(config{dir: t.TempDir()})
					if err != nil {
						t.Fatal(err)
					}
					defer st.close()
					req := request{
						ID:       1,
						Command:  cmdPut,
						ActionID: sha256Sum([]byte("original")),
						OutputID: sha256Sum(body),
						BodySize: int64(size),
					}
					original, err := st.put(req, bufio.NewReader(encodedBody(body)))
					if err != nil {
						t.Fatal(err)
					}
					if mode == "no-file" {
						if err := os.Remove(original.DiskPath); err != nil {
							t.Fatal(err)
						}
					}
					req.ID = 2
					req.ActionID = sha256Sum([]byte("invalid"))
					br := bufio.NewReader(strings.NewReader(tt.input + "\n{\"ID\":99,\"Command\":\"close\"}\n"))
					if _, err := st.put(req, br); err == nil {
						t.Fatal("put accepted an invalid body")
					}
					next, err := readRequest(br)
					if err != nil || next.ID != 99 {
						t.Fatalf("invalid body was not drained: next=%+v, err=%v", next, err)
					}
					if mode == "existing-file" {
						got, err := os.ReadFile(original.DiskPath)
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(got, body) {
							t.Fatal("invalid put changed the existing stored file")
						}
					} else if _, err := os.Stat(original.DiskPath); !os.IsNotExist(err) {
						t.Fatalf("invalid put left a stored file: stat err = %v", err)
					}
					shard, err := os.ReadDir(filepath.Dir(original.DiskPath))
					if err != nil {
						t.Fatal(err)
					}
					for _, file := range shard {
						if strings.Contains(file.Name(), tempMarker) {
							t.Fatalf("invalid put left a temp file: %s", file.Name())
						}
					}
					files, err := os.ReadDir(st.runDir)
					if err != nil {
						t.Fatal(err)
					}
					if len(files) != 1 || files[0].Name() != "run.lock" {
						t.Fatalf("invalid put left run folder files: %v", files)
					}
					if _, found, err := st.lookupEntry(hexOf(req.ActionID)); err != nil || found {
						t.Fatalf("invalid put registered an action: found=%t, err=%v", found, err)
					}
				})
			}
		}
	}
}

func BenchmarkPutSharedOutput(b *testing.B) {
	for _, size := range []int{0, 1 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			st, err := newStore(config{dir: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(st.close)
			body := bytes.Repeat([]byte("x"), size)
			var input []byte
			if size != 0 {
				input, err = io.ReadAll(encodedBody(body))
				if err != nil {
					b.Fatal(err)
				}
			}
			req := request{
				ID:       1,
				Command:  cmdPut,
				ActionID: sha256Sum([]byte("original")),
				OutputID: sha256Sum(body),
				BodySize: int64(size),
			}
			if _, err := st.put(req, bufio.NewReader(bytes.NewReader(input))); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := range b.N {
				req.ID = int64(i + 2)
				req.ActionID = sha256Sum([]byte(fmt.Sprintf("action %d", i)))
				if _, err := st.put(req, bufio.NewReader(bytes.NewReader(input))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestCacheRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.put(request{ID: 1, Command: cmdPut}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing ActionID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing OutputID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: -1,
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted negative body size")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("x")))); err == nil {
		t.Fatal("put accepted mismatched body size")
	}
	if _, err := st.get(request{ID: 2, Command: cmdGet}); err == nil {
		t.Fatal("get accepted missing ActionID")
	}
}

func TestPutDrainsBodyOnSetupError(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	notDir := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(notDir, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	st.outputsDir = notDir

	var input bytes.Buffer
	input.Write(encodedBody([]byte("body")).Bytes())
	writeJSON(t, &input, request{ID: 2, Command: cmdClose})
	br := bufio.NewReader(&input)

	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{41}, 32),
		OutputID: bytes.Repeat([]byte{42}, 32),
		BodySize: 4,
	}, br)
	if err == nil {
		t.Fatal("put succeeded with invalid outputs dir")
	}

	req, err := readRequest(br)
	if err != nil {
		t.Fatal(err)
	}
	if req.ID != 2 || req.Command != cmdClose {
		t.Fatalf("next request = %+v, want close request", req)
	}
}

func TestVersionedLayout(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	wantVersionDir := filepath.Join(cacheDir, "v2")
	if st.versionDir != wantVersionDir {
		t.Fatalf("versionDir = %q, want %q", st.versionDir, wantVersionDir)
	}
	if st.outputsDir != filepath.Join(wantVersionDir, "outputs") {
		t.Fatalf("outputsDir = %q, want under version dir", st.outputsDir)
	}
	if st.liveRoot != filepath.Join(wantVersionDir, "live") {
		t.Fatalf("liveRoot = %q, want under version dir", st.liveRoot)
	}
	if _, err := os.Stat(filepath.Join(wantVersionDir, "cache.db")); err != nil {
		t.Fatal(err)
	}
	var version int
	queryDB(t, st.db, `PRAGMA user_version`, nil, &version)
	if version != cacheSchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, cacheSchemaVersion)
	}
	var synchronous int
	queryDB(t, st.db, `PRAGMA synchronous`, nil, &synchronous)
	if synchronous != 1 {
		t.Fatalf("synchronous = %d, want 1 (NORMAL)", synchronous)
	}
}

func TestRejectsMismatchedDBVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	execDB(t, db, `PRAGMA user_version = 999`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(dbPath)
	if err == nil {
		_ = db.Close()
		t.Fatal("openDB succeeded with an unsupported user_version")
	}
}

func TestSQLiteBusyWaitRespectsContext(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	lockConn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite|sqlite.OpenURI)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close() //nolint:errcheck
	if err := execute(lockConn, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer execute(lockConn, `ROLLBACK`) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = db.withConn(ctx, func(conn *sqlite.Conn) error {
		return execute(conn, `
INSERT INTO runs(run_id, path, lock_path, created_at)
VALUES ('blocked', '', '', 0)`)
	})
	if code := sqlite.ErrCode(err).ToPrimary(); code != sqlite.ResultBusy {
		t.Fatalf("write error = %v (code %v), want SQLITE_BUSY", err, code)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("write observed context cancellation after %v, want under 1s", elapsed)
	}
}

func TestSQLiteBusyWaitsUntilLockReleased(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck

	lockConn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite|sqlite.OpenURI)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close() //nolint:errcheck
	if err := execute(lockConn, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- db.withConn(context.Background(), func(conn *sqlite.Conn) error {
			return execute(conn, `
INSERT INTO runs(run_id, path, lock_path, created_at)
VALUES ('blocked', '', '', 0)`)
		})
	}()

	select {
	case err := <-done:
		t.Fatalf("write returned while database was locked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := execute(lockConn, `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write remained blocked after database lock was released")
	}
}

func TestReclaimsAbandonedUnlockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{18}, 32)
	outputID := bytes.Repeat([]byte{19}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	runID := st.runID
	runDir := st.runDir
	storedPath := res.DiskPath
	abandonStore(t, st)

	st, err = newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir still exists: err=%v", err)
	}
	if _, err := os.Stat(storedPath); err != nil {
		t.Fatalf("stored file of the abandoned run was removed: %v", err)
	}
	if got := countRows(t, st.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID); got != 0 {
		t.Fatalf("abandoned run rows = %d, want 0", got)
	}
}

func TestKeepsLockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st1, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st1.close()
	actionID := bytes.Repeat([]byte{20}, 32)
	outputID := bytes.Repeat([]byte{21}, 32)
	if _, err := st1.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	st2, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.close()

	if _, err := os.Stat(st1.runDir); err != nil {
		t.Fatalf("locked run dir was removed: %v", err)
	}
	if got := countRows(t, st2.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, st1.runID); got != 1 {
		t.Fatalf("locked run rows = %d, want 1", got)
	}
}

func TestCleanupDropsMissingRunRecord(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	missingRunDir := filepath.Join(st.liveRoot, "run-missing")
	missingLock := filepath.Join(missingRunDir, "run.lock")
	if err := st.q.registerRun(context.Background(), "run-missing", missingRunDir, missingLock, unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := st.cleanupAbandonedRuns(); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, st.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, "run-missing"); got != 0 {
		t.Fatalf("missing run rows = %d, want 0", got)
	}
}

func TestCloseDropsLiveFiles(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{5}, 32)
	outputID := bytes.Repeat([]byte{6}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatal(err)
	}
	runDir := st.runDir
	st.close()
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("live run dir still exists: err=%v", err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatalf("stored file was removed at close: %v", err)
	}
}

func TestIndexedExportDataDetectionRejectsText(t *testing.T) {
	t.Parallel()

	for _, data := range [][]byte{
		nil,
		[]byte("i"),
		[]byte("i\x00\x00\x00"),
		[]byte("important text"),
		[]byte("i\x03\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x00"),
	} {
		if isIndexedExportData(data) {
			t.Fatalf("isIndexedExportData(%q) = true", data)
		}
	}
}

func TestPruneKeepsLiveBlobs(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{7}, 32)
	outputID := bytes.Repeat([]byte{8}, 32)
	body := forkBody(70)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	if size := compressedSizeForTest(t, st); size <= st.maxSize {
		t.Fatalf("stored size = %d, want above maxSize %d", size, st.maxSize)
	}
	execDB(t, st.db, `UPDATE entries SET accessed_at = ?`, unixMillis(time.Now().Add(-2*time.Hour)))
	// The run stays registered: an active run blocks every deletion, however
	// old the entry is.
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := st.lookupEntry(hexOf(actionID)); err != nil || !found {
		t.Fatalf("live entry was pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(st.storedPath(hexOf(outputID))); err != nil {
		t.Fatalf("live stored file was pruned: %v", err)
	}
}

func TestPruneUsesLifecycleLock(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	done := make(chan error, 1)
	go func() {
		done <- st.prune()
	}()

	select {
	case err := <-done:
		t.Fatalf("prune finished while lifecycle lock was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticPruneSkipsRecentFullPrune(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	outputID := strings.Repeat("a", 64)
	orphanPath := writeOrphanOutput(t, st, outputID)
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	now := time.Now()
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now)); err != nil {
		t.Fatal(err)
	}

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("recently pruned cache was pruned again: %v", err)
	}
}

func TestAutomaticPruneRunsFullPruneAfterInterval(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	outputID := strings.Repeat("a", 64)
	orphanPath := writeOrphanOutput(t, st, outputID)
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	now := time.Now()
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now.Add(-fullPruneInterval-time.Minute))); err != nil {
		t.Fatal(err)
	}

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan stat err = %v, want not exist", err)
	}
	lastMillis, found, err := st.q.fullPruneState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("last full prune was not recorded")
	}
	if last := millisTime(lastMillis); last.Before(now.Truncate(time.Millisecond)) {
		t.Fatalf("last full prune = %v, want at or after %v", last, now)
	}
}

func TestAutomaticPruneDoesNotRecordSkippedPrune(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st1, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer st1.close()
	st2, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.close()

	execDB(t, st1.db, `DELETE FROM runs WHERE run_id = ?`, st1.runID)
	if err := st1.pruneAutomatically(time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := st1.q.fullPruneState(context.Background()); err != nil || found {
		t.Fatalf("last full prune: found=%t, err=%v; want not found", found, err)
	}
}

func TestAutomaticPruneEnforcesSizeBetweenFullPrunes(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 320,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	outputIDs := addSizedCacheEntries(t, st, now.Add(-2*time.Hour), 4, 100)
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now)); err != nil {
		t.Fatal(err)
	}

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	if total != 200 {
		t.Fatalf("compressed size after prune = %d, want 200", total)
	}
	for _, outputID := range outputIDs[:2] {
		if _, err := os.Stat(st.storedPath(outputID)); !os.IsNotExist(err) {
			t.Fatalf("old output %s stat err = %v, want not exist", outputID, err)
		}
	}
	for _, outputID := range outputIDs[2:] {
		if _, err := os.Stat(st.storedPath(outputID)); err != nil {
			t.Fatalf("new output %s was pruned: %v", outputID, err)
		}
	}
}

func TestAutomaticPruneDoesNotPruneBelowMaximum(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 320,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	outputIDs := addSizedCacheEntries(t, st, now, 3, 100)
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now)); err != nil {
		t.Fatal(err)
	}

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	if total, err := st.compressedSize(); err != nil || total != 300 {
		t.Fatalf("compressed size = %d, err=%v; want 300", total, err)
	}
	for _, outputID := range outputIDs {
		if _, err := os.Stat(st.storedPath(outputID)); err != nil {
			t.Fatalf("output %s was pruned below maxSize: %v", outputID, err)
		}
	}
}

func TestAutomaticPruneTreatsFutureTimestampAsDue(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	orphanPath := writeOrphanOutput(t, st, strings.Repeat("f", 64))
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	now := time.Now()
	if err := st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}

	if err := st.pruneAutomatically(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan stat err = %v, want not exist", err)
	}
}

func TestCompressedSizeTracksDistinctOutputs(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	output1 := strings.Repeat("1", 64)
	output2 := strings.Repeat("2", 64)
	entry1 := entry{
		ActionID:       strings.Repeat("a", 64),
		OutputID:       output1,
		Size:           100,
		CompressedSize: 40,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.q.upsertEntry(context.Background(), entry1, nil); err != nil {
		t.Fatal(err)
	}
	entry2 := entry1
	entry2.ActionID = strings.Repeat("b", 64)
	if err := st.q.upsertEntry(context.Background(), entry2, nil); err != nil {
		t.Fatal(err)
	}
	if got := compressedSizeForTest(t, st); got != 40 {
		t.Fatalf("compressed size with shared output = %d, want 40", got)
	}

	entry1.OutputID = output2
	entry1.CompressedSize = 60
	if err := st.q.upsertEntry(context.Background(), entry1, nil); err != nil {
		t.Fatal(err)
	}
	if got := compressedSizeForTest(t, st); got != 100 {
		t.Fatalf("compressed size after output change = %d, want 100", got)
	}

	if err := st.q.deleteEntriesByOutputID(context.Background(), output1); err != nil {
		t.Fatal(err)
	}
	if got := compressedSizeForTest(t, st); got != 60 {
		t.Fatalf("compressed size after shared output deletion = %d, want 60", got)
	}
	if err := st.q.deleteEntriesByOutputID(context.Background(), output2); err != nil {
		t.Fatal(err)
	}
	if got := compressedSizeForTest(t, st); got != 0 {
		t.Fatalf("compressed size after all output deletion = %d, want 0", got)
	}
}

func TestConcurrentStoresUpdateCompressedSize(t *testing.T) {
	cacheDir := t.TempDir()
	const (
		storeCount  = 8
		putCount    = 200
		outputCount = 16
	)

	stores := make([]*store, storeCount)
	for i := range stores {
		st, err := newStore(config{dir: cacheDir})
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = st
		defer st.close()
	}

	start := make(chan struct{})
	errs := make(chan error, storeCount)
	var wg sync.WaitGroup
	for worker, st := range stores {
		wg.Go(func() {
			<-start
			for i := range putCount {
				output := (worker*putCount + i) % outputCount
				ent := entry{
					ActionID:       fmt.Sprintf("%064x", worker*putCount+i+1),
					OutputID:       fmt.Sprintf("%064x", output+1),
					Size:           int64(output + 1),
					CompressedSize: int64(output + 1),
					CreatedAt:      time.Now(),
					AccessedAt:     time.Now(),
				}
				if err := st.q.upsertEntry(context.Background(), ent, nil); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	const want = outputCount * (outputCount + 1) / 2
	if got := compressedSizeForTest(t, stores[0]); got != want {
		t.Fatalf("compressed size = %d, want %d", got, want)
	}
	reconciled, err := stores[0].q.reconcileCompressedSize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reconciled != want {
		t.Fatalf("reconciled compressed size = %d, want %d", reconciled, want)
	}
}

func BenchmarkUpsertEntrySharedOutput(b *testing.B) {
	for _, actions := range []int{1, 75000} {
		b.Run(fmt.Sprintf("actions=%d", actions), func(b *testing.B) {
			st, ent := benchmarkSharedOutputStore(b, actions)

			b.ResetTimer()
			for range b.N {
				if err := st.q.upsertEntry(context.Background(), ent, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			size, err := st.compressedSize()
			if err != nil {
				b.Fatal(err)
			}
			if size != ent.CompressedSize {
				b.Fatalf("compressed size = %d, want %d", size, ent.CompressedSize)
			}
		})
	}
}

func benchmarkSharedOutputStore(b *testing.B, actions int) (*store, entry) {
	b.Helper()
	st, err := newStore(config{dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(st.close)
	now := time.Now()
	ent := entry{
		ActionID:       fmt.Sprintf("%064x", 1),
		OutputID:       hexOf(sha256Sum(nil)),
		CompressedSize: 13,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.q.upsertEntry(context.Background(), ent, nil); err != nil {
		b.Fatal(err)
	}
	if err := st.db.withTx(context.Background(), func(conn *sqlite.Conn) error {
		return execute(conn, `
WITH RECURSIVE actions(n) AS (
	SELECT 2 WHERE ? >= 2
	UNION ALL
	SELECT n + 1 FROM actions WHERE n < ?
)
INSERT INTO entries(action_id, output_id, size, compressed_size, created_at, accessed_at)
SELECT printf('%064x', n), ?, 0, 13, ?, ? FROM actions`,
			actions, actions, ent.OutputID, unixMillis(now), unixMillis(now))
	}); err != nil {
		b.Fatal(err)
	}
	return st, ent
}

func TestCompressedSizeRejectsNegativeAdjustment(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	ent := entry{
		ActionID:       strings.Repeat("a", 64),
		OutputID:       strings.Repeat("1", 64),
		Size:           100,
		CompressedSize: 40,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.q.upsertEntry(context.Background(), ent, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.q.setState(context.Background(), compressedSizeStateKey, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.q.deleteEntriesByOutputID(context.Background(), ent.OutputID); err == nil {
		t.Fatal("delete succeeded with an understated compressed-size counter")
	}
	if _, found, err := st.q.lookupEntry(context.Background(), ent.ActionID); err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("failed accounting adjustment did not roll back entry deletion")
	}
}

func TestFullPruneReconcilesCompressedSize(t *testing.T) {
	t.Parallel()

	for _, mode := range []struct {
		name  string
		prune func(*store, time.Time) error
	}{
		// The walk then replaces the counter with the stored total (T10), so this
		// test stops at the row-based reconciliation that runs before it.
		{"old-data", (*store).pruneOldDataLocked},
	} {
		for _, maxAge := range []time.Duration{0, defaultMaxAge} {
			for _, expired := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/age=%s/expired=%t", mode.name, maxAge, expired), func(t *testing.T) {
					t.Parallel()
					st, err := newStore(config{
						dir:     t.TempDir(),
						maxSize: 1000,
						maxAge:  maxAge,
					})
					if err != nil {
						t.Fatal(err)
					}
					defer st.close()
					now := time.Now()
					outputs := addSizedCacheEntries(t, st, now, 2, 100)
					wantSize := int64(200)
					wantEntries := int64(2)
					if expired {
						old := trimCutoff(defaultMaxAge, now).Add(-time.Minute)
						execDB(t, st.db, `UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
							unixMillis(old), fmt.Sprintf("%064x", 1))
						if err := st.upsertEntry(entry{
							ActionID:       fmt.Sprintf("%064x", 3),
							OutputID:       outputs[1],
							Size:           100,
							CompressedSize: 100,
							CreatedAt:      old,
							AccessedAt:     old,
						}); err != nil {
							t.Fatal(err)
						}
						wantEntries = 3
						if maxAge > 0 {
							wantSize, wantEntries = 100, 1
						}
					}
					if err := st.q.setState(context.Background(), compressedSizeStateKey, 1); err != nil {
						t.Fatal(err)
					}
					execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
					if err := st.q.setState(context.Background(), lastFullPruneStateKey, 0); err != nil {
						t.Fatal(err)
					}
					if err := st.db.withConn(context.Background(), func(conn *sqlite.Conn) error {
						return sqlitex.ExecuteScript(conn, `
CREATE TABLE size_reconciliations(value INTEGER);
CREATE TRIGGER log_size_reconciliation AFTER UPDATE OF value ON state
WHEN NEW.key = 'compressed-size'
BEGIN
	INSERT INTO size_reconciliations VALUES (NEW.value);
END;`, nil)
					}); err != nil {
						t.Fatal(err)
					}
					if err := mode.prune(st, now); err != nil {
						t.Fatal(err)
					}
					if got := compressedSizeForTest(t, st); got != wantSize {
						t.Fatalf("reconciled compressed size = %d, want %d", got, wantSize)
					}
					var reconciliations, entries int64
					queryDB(t, st.db, `SELECT COUNT(*) FROM size_reconciliations`, nil, &reconciliations)
					if reconciliations != 1 {
						t.Fatalf("full prune reconciled size %d times, want 1", reconciliations)
					}
					queryDB(t, st.db, `SELECT COUNT(*) FROM entries`, nil, &entries)
					if entries != wantEntries {
						t.Fatalf("remaining entries = %d, want %d", entries, wantEntries)
					}
					removed, err := st.q.deleteEntriesAccessedBefore(context.Background(), unixMillis(now.Add(defaultMaxAge)))
					if err != nil {
						t.Fatal(err)
					}
					if removed != wantEntries || compressedSizeForTest(t, st) != 0 {
						t.Fatalf("direct age deletion did not reconcile size: removed=%d, want %d", removed, wantEntries)
					}
				})
			}
		}
	}
}

func BenchmarkFullPrune(b *testing.B) {
	for _, actions := range []int{1, 75000} {
		b.Run(fmt.Sprintf("actions=%d", actions), func(b *testing.B) {
			st, _ := benchmarkSharedOutputStore(b, actions)
			st.maxAge = defaultMaxAge
			if err := st.q.deleteRun(context.Background(), st.runID); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				if err := st.prune(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestEntryAccessesFlushAtThreshold(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	outputIDs := addSizedCacheEntries(t, st, now.Add(-time.Hour), accessFlushThreshold, 1)
	st.lastAccessFlush = now.Add(-accessFlushMinInterval)
	for i := range accessFlushThreshold {
		if err := st.markEntryAccessAt(fmt.Sprintf("%064x", i+1), now.Add(time.Duration(i)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}

	var accessedAt int64
	queryDB(t, st.db, `SELECT accessed_at FROM entries WHERE output_id = ?`, []any{outputIDs[0]}, &accessedAt)
	if accessedAt != unixMillis(now) {
		t.Fatalf("accessed_at = %d, want %d", accessedAt, unixMillis(now))
	}
	if len(st.accessed) != 0 {
		t.Fatalf("buffered accesses = %d, want 0", len(st.accessed))
	}
}

func TestEntryAccessesFlushAfterInterval(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	addSizedCacheEntries(t, st, now.Add(-time.Hour), 1, 1)
	st.lastAccessFlush = now.Add(-accessFlushInterval)
	if err := st.markEntryAccessAt(fmt.Sprintf("%064x", 1), now); err != nil {
		t.Fatal(err)
	}

	var accessedAt int64
	queryDB(t, st.db, `SELECT accessed_at FROM entries WHERE action_id = ?`, []any{fmt.Sprintf("%064x", 1)}, &accessedAt)
	if accessedAt != unixMillis(now) {
		t.Fatalf("accessed_at = %d, want %d", accessedAt, unixMillis(now))
	}
}

func TestEntryAccessFlushFailureRetainsPendingAccesses(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	st.lastAccessFlush = now.Add(-accessFlushMinInterval)
	for i := range accessFlushThreshold - 1 {
		st.accessed[fmt.Sprintf("%064x", i+1)] = unixMillis(now)
	}
	abandonStore(t, st)

	actionID := fmt.Sprintf("%064x", accessFlushThreshold)
	if err := st.markEntryAccessAt(actionID, now); err == nil {
		t.Fatal("markEntryAccessAt succeeded with closed database")
	}
	st.mu.Lock()
	accessedAt, found := st.accessed[actionID]
	st.mu.Unlock()
	if !found || accessedAt != unixMillis(now) {
		t.Fatalf("pending access = (%d, %t), want (%d, true)", accessedAt, found, unixMillis(now))
	}

	if err := st.markEntryAccessAt(fmt.Sprintf("%064x", accessFlushThreshold+1), now.Add(accessFlushMinInterval/2)); err != nil {
		t.Fatalf("access flush retried before minimum interval: %v", err)
	}
}

func TestEntryAccessTimesDoNotMoveBackward(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	now := time.Now()
	addSizedCacheEntries(t, st, now, 1, 1)
	actionID := fmt.Sprintf("%064x", 1)
	if err := st.db.withTx(context.Background(), func(conn *sqlite.Conn) error {
		return touchEntries(conn, map[string]int64{actionID: unixMillis(now.Add(-time.Hour))})
	}); err != nil {
		t.Fatal(err)
	}

	var accessedAt int64
	queryDB(t, st.db, `SELECT accessed_at FROM entries WHERE action_id = ?`, []any{actionID}, &accessedAt)
	if accessedAt != unixMillis(now) {
		t.Fatalf("accessed_at = %d, want %d", accessedAt, unixMillis(now))
	}
}

func TestNewStoreUsesLifecycleLock(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cacheDir, "v2"), 0o777); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(cacheDir, "v2", "lifecycle.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	type result struct {
		st  *store
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := newStore(config{dir: cacheDir})
		done <- result{st: st, err: err}
	}()

	select {
	case res := <-done:
		if res.st != nil {
			res.st.close()
		}
		t.Fatalf("newStore finished while lifecycle lock was held: %v", res.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	res.st.close()
}

func TestPruneRemovesUnusedBlobs(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{9}, 32)
	outputID := bytes.Repeat([]byte{10}, 32)
	body := forkBody(71)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	storedPath := st.storedPath(hexOf(outputID))
	st.close()

	st, err = newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	execDB(t, st.db, `UPDATE entries SET accessed_at = ?`, unixMillis(time.Now().Add(-2*time.Hour)))
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := st.lookupEntry(hexOf(actionID)); err != nil || found {
		t.Fatalf("entry was not pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(storedPath); !os.IsNotExist(err) {
		t.Fatalf("stored file was not pruned: %v", err)
	}
}

func TestPruneRemovesOrphanBlobsWithSizePruningDisabled(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	orphanPath := writeOrphanOutput(t, st, strings.Repeat("a", 64))
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan stat err = %v, want not exist", err)
	}
}

func TestCatalogQueriesRespectCanceledContext(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.q.listOtherRuns(ctx, st.runID); err == nil {
		t.Fatal("listOtherRuns succeeded with canceled context")
	}
	if _, err := st.q.pruneCandidates(ctx, 0); err == nil {
		t.Fatal("pruneCandidates succeeded with canceled context")
	}
	if err := st.q.referencedOutputIDs(ctx, "00", "01", make(map[string]struct{})); err == nil {
		t.Fatal("referencedOutputIDs succeeded with canceled context")
	}
}

func TestPruneUsesBlobLRU(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	sharedOutputID := bytes.Repeat([]byte{11}, 32)
	oldSharedActionID := bytes.Repeat([]byte{12}, 32)
	newSharedActionID := bytes.Repeat([]byte{13}, 32)
	prunedOutputID := bytes.Repeat([]byte{14}, 32)
	prunedActionID := bytes.Repeat([]byte{15}, 32)

	for _, tc := range []struct {
		actionID []byte
		outputID []byte
		body     []byte
	}{
		{oldSharedActionID, sharedOutputID, forkBody(1)},
		{newSharedActionID, sharedOutputID, forkBody(1)},
		{prunedActionID, prunedOutputID, forkBody(2)},
	} {
		forkPut(t, st, tc.actionID, tc.outputID, tc.body)
	}

	// Every row is older than the 1 hour floor, so the size prune may remove it.
	recent := unixMillis(time.Now().Add(-2 * time.Hour))
	execDB(t, st.db,
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		recent-3000,
		hexOf(oldSharedActionID),
	)
	execDB(t, st.db,
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		recent-1000,
		hexOf(newSharedActionID),
	)
	execDB(t, st.db,
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		recent-2000,
		hexOf(prunedActionID),
	)

	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	// The prune first sets the counter to the bytes on disk under v2/, catalog
	// files included. The limit allows the shared output and the catalog, and
	// not the pruned output; the slack covers catalog growth during the prune.
	total, err := storedBytesUnder(st.versionDir)
	if err != nil {
		t.Fatal(err)
	}
	prunedBytes := forkStoredBytes(forkLstat(t, st.storedPath(hexOf(prunedOutputID))))
	st.maxSize = total - prunedBytes + 64<<10
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, found, err := st.lookupEntry(hexOf(prunedActionID)); err != nil || found {
		t.Fatalf("middle-aged blob was not pruned: found=%t, err=%v", found, err)
	}
	if _, found, err := st.lookupEntry(hexOf(oldSharedActionID)); err != nil || !found {
		t.Fatalf("shared output old action was pruned: found=%t, err=%v", found, err)
	}
	if _, found, err := st.lookupEntry(hexOf(newSharedActionID)); err != nil || !found {
		t.Fatalf("shared output new action was pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(st.storedPath(hexOf(sharedOutputID))); err != nil {
		t.Fatalf("shared output blob was pruned: %v", err)
	}
}

func TestPruneRemovesEntriesOlderThanTrimLimit(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
		maxAge:  defaultMaxAge,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	oldActionID := bytes.Repeat([]byte{20}, 32)
	oldOutputID := bytes.Repeat([]byte{21}, 32)
	freshActionID := bytes.Repeat([]byte{22}, 32)
	freshOutputID := bytes.Repeat([]byte{23}, 32)
	sharedOutputID := bytes.Repeat([]byte{24}, 32)
	oldSharedActionID := bytes.Repeat([]byte{25}, 32)
	freshSharedActionID := bytes.Repeat([]byte{26}, 32)

	for _, tc := range []struct {
		actionID []byte
		outputID []byte
		body     []byte
	}{
		{oldActionID, oldOutputID, forkBody(1)},
		{freshActionID, freshOutputID, forkBody(2)},
		{oldSharedActionID, sharedOutputID, forkBody(3)},
		{freshSharedActionID, sharedOutputID, forkBody(3)},
	} {
		forkPut(t, st, tc.actionID, tc.outputID, tc.body)
	}

	stale := unixMillis(trimCutoff(defaultMaxAge, time.Now())) - int64(time.Minute/time.Millisecond)
	for _, actionID := range [][]byte{oldActionID, oldSharedActionID} {
		execDB(t, st.db,
			`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
			stale,
			hexOf(actionID),
		)
	}

	// The walk removes the file of a deleted row once its mtime is past the floor.
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(st.storedPath(hexOf(oldOutputID)), twoHoursAgo, twoHoursAgo); err != nil {
		t.Fatal(err)
	}

	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, found, err := st.lookupEntry(hexOf(oldActionID)); err != nil || found {
		t.Fatalf("stale entry was not pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(st.storedPath(hexOf(oldOutputID))); !os.IsNotExist(err) {
		t.Fatalf("stale entry blob stat err = %v, want not exist", err)
	}
	if _, found, err := st.lookupEntry(hexOf(freshActionID)); err != nil || !found {
		t.Fatalf("fresh entry was pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(st.storedPath(hexOf(freshOutputID))); err != nil {
		t.Fatalf("fresh entry blob was pruned: %v", err)
	}
	if _, found, err := st.lookupEntry(hexOf(oldSharedActionID)); err != nil || found {
		t.Fatalf("stale shared action was not pruned: found=%t, err=%v", found, err)
	}
	if _, found, err := st.lookupEntry(hexOf(freshSharedActionID)); err != nil || !found {
		t.Fatalf("fresh shared action was pruned: found=%t, err=%v", found, err)
	}
	if _, err := os.Stat(st.storedPath(hexOf(sharedOutputID))); err != nil {
		t.Fatalf("shared output blob was pruned while still referenced: %v", err)
	}
}

func TestPruneKeepsOldEntriesWhenMaxAgeDisabled(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
		maxAge:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{40}, 32)
	outputID := bytes.Repeat([]byte{41}, 32)
	body := bytes.Repeat([]byte("x"), 64)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}

	execDB(t, st.db,
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(1000),
		hexOf(actionID),
	)
	execDB(t, st.db, `DELETE FROM runs WHERE run_id = ?`, st.runID)
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, found, err := st.lookupEntry(hexOf(actionID)); err != nil || !found {
		t.Fatalf("entry pruned with age-based pruning disabled: found=%t, err=%v", found, err)
	}
}

func TestAccessTimesFlushOnClose(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{30}, 32)
	outputID := bytes.Repeat([]byte{31}, 32)
	body := []byte("access body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	actionHex := hexOf(actionID)
	execDB(t, st.db,
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(1000),
		actionHex,
	)
	if _, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if err := st.flushAccessTimes(); err != nil {
		t.Fatal(err)
	}
	var accessedAt int64
	queryDB(t, st.db,
		`SELECT accessed_at FROM entries WHERE action_id = ?`,
		[]any{actionHex},
		&accessedAt,
	)
	if accessedAt <= 1000 {
		t.Fatalf("accessed_at = %d, want > 1000", accessedAt)
	}
}

func TestRunProtocol(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{3}, 32)
	outputID := bytes.Repeat([]byte{4}, 32)
	body := []byte("protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	writeJSON(t, &stdin, request{
		ID:       2,
		Command:  cmdGet,
		ActionID: actionID,
	})
	writeJSON(t, &stdin, request{ID: 3, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" || putRes.DiskPath == "" {
		t.Fatalf("put response = %+v", putRes)
	}
	var getRes response
	if err := dec.Decode(&getRes); err != nil {
		t.Fatal(err)
	}
	if getRes.ID != 2 || getRes.Err != "" || getRes.Miss || !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("get response = %+v", getRes)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 3 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunProtocolHandlesFinalRequestWithoutNewline(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 1 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunReturnsAfterEOF(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	var hello response
	if err := json.NewDecoder(&stdout).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
}

func TestRunRejectsBadProfilePath(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{
		"-dir", t.TempDir(),
		"-cpuprofile", t.TempDir(),
	}, strings.NewReader(""), &stdout)
	if err == nil {
		t.Fatal("run accepted directory CPU profile path")
	}
}

func TestRunRejectsBadArgsAndCacheDir(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-bad"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted bad args")
	}
	if err := run([]string{"wat"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted unexpected argument")
	}
	if err := run([]string{"-dir", t.TempDir(), "status"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted flags before subcommand")
	}

	cacheFile := filepath.Join(t.TempDir(), "cache-file")
	if err := os.WriteFile(cacheFile, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"-dir", cacheFile}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted file cache dir")
	}
}

func TestRunHelp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "root", args: []string{"-h"}, want: "Usage:\n  gocachez [flags]\n"},
		{name: "clean", args: []string{"clean", "-h"}, want: "Usage:\n  gocachez clean [flags]\n"},
		{name: "status", args: []string{"status", "-dir", t.TempDir(), "-h"}, want: "Usage:\n  gocachez status [flags]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			if err := run(tc.args, strings.NewReader(""), &stdout); err != nil {
				t.Fatal(err)
			}
			assertContains(t, stdout.String(), tc.want)
			assertContains(t, stdout.String(), "-h")
		})
	}
}

func TestRunHelpDoesNotCreateCacheState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := run([]string{"clean", "-h", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "v2")); !os.IsNotExist(err) {
		t.Fatalf("help created cache state: %v", err)
	}
}

func TestRunCleanRemovesInactiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{43}, 32)
	outputID := bytes.Repeat([]byte{44}, 32)
	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	storedFile := st.storedPath(hexOf(outputID))
	dbPath := filepath.Join(st.versionDir, "cache.db")
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("clean stdout = %q, want empty", stdout.String())
	}
	if _, err := os.Stat(storedFile); !os.IsNotExist(err) {
		t.Fatalf("blob stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("catalog stat err = %v, want not exist", err)
	}
}

func TestRunCleanKeepsActiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{45}, 32)
	outputID := bytes.Repeat([]byte{46}, 32)
	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	storedFile := st.storedPath(hexOf(outputID))

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(storedFile); err != nil {
		t.Fatalf("active blob was removed: %v", err)
	}
	if _, found, err := st.lookupEntry(hexOf(actionID)); err != nil || !found {
		t.Fatalf("active entry was removed: found=%t, err=%v", found, err)
	}
}

func TestRunCleanRemovesAbandonedState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{47}, 32)
	outputID := bytes.Repeat([]byte{48}, 32)
	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	storedFile := st.storedPath(hexOf(outputID))
	runDir := st.runDir
	abandonStore(t, st)

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(storedFile); !os.IsNotExist(err) {
		t.Fatalf("abandoned blob stat err = %v, want not exist", err)
	}
}

func TestRunStatusEmptyCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Configuration:\n")
	assertContains(t, got, "Cache directory")
	assertContains(t, got, cacheDir)
	assertContains(t, got, "Max size")
	assertContains(t, got, "20.0GiB")
	assertContains(t, got, "Max age            5d")
	assertContains(t, got, "Verbose")
	assertContains(t, got, "false")
	assertContains(t, got, "Summary:\n")
	assertContains(t, got, "State               missing")
	assertContains(t, got, "Cached actions      0")
	assertContains(t, got, "Cached outputs      0")
	assertContains(t, got, "Oldest cache entry  n/a")
	assertContains(t, got, "Live runs           0 active, 0 inactive")
	assertContains(t, got, "Storage:\n")
	assertContains(t, got, "Original output size    0B")
	assertContains(t, got, "Compressed cache blobs  0B (0 files)")
	assertContains(t, got, "Blob max usage          0B / 20.0GiB (0.0%, 20.0GiB remaining)")
	assertContains(t, got, "Blob-only savings       0B (0.0%)")
	assertContains(t, got, "Compressed blob contents:\n")
	assertContains(t, got, "None      0        0B      0B  0B (0.0%)")
}

func TestStatusDoesNotWaitForLifecycleLock(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	done := make(chan error, 1)
	go func() {
		_, err := readStatus(config{dir: cacheDir})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("status waited for lifecycle lock")
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestRunStatusShowsEffectiveConfig(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir, "-max-size", "1MiB", "-max-age", "2d", "-v"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Cache directory")
	assertContains(t, got, cacheDir)
	assertContains(t, got, "Max size           1.0MiB")
	assertContains(t, got, "Max age            2d")
	assertContains(t, got, "Verbose            true")
}

func TestRunStatusInactiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{49}, 32)
	outputID := bytes.Repeat([]byte{50}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "State               present")
	assertContains(t, got, "Cached actions      1")
	assertContains(t, got, "Cached outputs      1")
	assertContains(t, got, "Oldest cache entry  <1m")
	assertContains(t, got, "Live runs           0 active, 0 inactive")
	assertContains(t, got, "Original output size")
	assertContains(t, got, "Compressed cache blobs")
	assertContains(t, got, "Blob max usage")
	assertContains(t, got, "Blob-only savings")
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Text files      1        4B")
}

func TestRunStatusActiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{51}, 32)
	outputID := bytes.Repeat([]byte{52}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "State               present")
	assertContains(t, got, "Cached actions      1")
	assertContains(t, got, "Cached outputs      1")
	assertContains(t, got, "Live runs           1 active, 0 inactive")
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Text files      1        4B")
}

func TestRunStatusShowsBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	exportData := []byte("uFAKE")
	archive := goArchive(goPkgdef(exportData), bytes.Repeat([]byte("object data"), 16))
	compilerID := append([]byte("/usr/lib/ccache/bin/gcc\x00stat 1448536 1ed 2026-05-13 07:28:29 -0700 PDT false\n\x00"),
		bytes.Repeat([]byte{0x86}, 32)...)
	entries := []struct {
		actionID byte
		outputID byte
		body     []byte
	}{
		{63, 64, archive},
		{61, 62, indexedExportData(t)},
		{65, 66, []byte("// Code generated by cmd/cgo; DO NOT EDIT.\n\npackage net\n\nconst x = 1\n")},
		{67, 68, []byte{0x7f, 'E', 'L', 'F', 1, 2, 3}},
		{71, 72, []byte("go index v2\n\x00\x01\x02")},
		{73, 74, compilerID},
		{75, 76, []byte{0x00, 'c', 'v', 'm', 1, 0, 0, 0}},
	}
	for _, entry := range entries {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: bytes.Repeat([]byte{entry.actionID}, 32),
			OutputID: bytes.Repeat([]byte{entry.outputID}, 32),
			BodySize: int64(len(entry.body)),
		}, bufio.NewReader(encodedBody(entry.body))); err != nil {
			t.Fatal(err)
		}
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Go package archives")
	assertContains(t, got, "Go indexed export data")
	assertContains(t, got, "Go package indexes")
	assertContains(t, got, "Generated cgo sources")
	assertContains(t, got, "ELF binaries")
	assertContains(t, got, "C compiler IDs")
	assertContains(t, got, "Go coverage metadata")
}

func TestRunStatusCountsUnreadableBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{69}, 32)
	outputID := bytes.Repeat([]byte{70}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	storedPath := st.storedPath(hexOf(outputID))
	st.close()
	if err := os.Remove(storedPath); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Compressed blob contents:")
	assertContains(t, got, "Unreadable blobs      1        4B")
}

func TestStatusCachesBlobTypes(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	archive := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	outputID := bytes.Repeat([]byte{90}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{89}, 32),
		OutputID: outputID,
		BodySize: int64(len(archive)),
	}, bufio.NewReader(encodedBody(archive))); err != nil {
		t.Fatal(err)
	}
	st.close()

	versionDir, outputsDir, _, _ := cachePaths(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")

	statuses, err := readBlobTypeStatus(dbPath, outputsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)

	// Removing the blob forces the second pass to rely on the cached
	// classification; if it decompressed it would report the blob unreadable.
	if err := os.Remove(storedPath(outputsDir, hexOf(outputID))); err != nil {
		t.Fatal(err)
	}
	statuses, err = readBlobTypeStatus(dbPath, outputsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)
}

func TestStatusReclassifiesWhenClassifierVersionChanges(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{dir: cacheDir})
	if err != nil {
		t.Fatal(err)
	}
	archive := goArchive(goPkgdef([]byte("uFAKE")), bytes.Repeat([]byte("object data"), 64))
	outputID := bytes.Repeat([]byte{92}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{91}, 32),
		OutputID: outputID,
		BodySize: int64(len(archive)),
	}, bufio.NewReader(encodedBody(archive))); err != nil {
		t.Fatal(err)
	}
	st.close()

	versionDir, outputsDir, _, _ := cachePaths(config{dir: cacheDir})
	dbPath := filepath.Join(versionDir, "cache.db")

	// Seed a wrong classification recorded under a different classifier version.
	execCatalog(t, dbPath, `UPDATE entries SET blob_type = ?, blob_type_version = ?`,
		int64(blobTypeText), int64(blobClassifierVersion+1))

	statuses, err := readBlobTypeStatus(dbPath, outputsDir)
	if err != nil {
		t.Fatal(err)
	}
	assertBlobKind(t, statuses, blobTypeGoPackageArchive, 1)

	// The stale value is recomputed and re-stored at the current version.
	var kind, version optionalInt64
	queryCatalog(t, dbPath, `SELECT blob_type, blob_type_version FROM entries WHERE output_id = ?`,
		[]any{hexOf(outputID)}, &kind, &version)
	if !kind.ok || blobTypeKind(kind.value) != blobTypeGoPackageArchive {
		t.Fatalf("cached blob_type = %v, want %d", kind, blobTypeGoPackageArchive)
	}
	if !version.ok || version.value != int64(blobClassifierVersion) {
		t.Fatalf("cached blob_type_version = %v, want %d", version, blobClassifierVersion)
	}
}

func TestUpsertEntryInvalidatesClassificationsOnOutputChange(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx := context.Background()
	actionID := hexOf(bytes.Repeat([]byte{1}, 32))
	output1 := hexOf(bytes.Repeat([]byte{2}, 32))
	output2 := hexOf(bytes.Repeat([]byte{3}, 32))
	now := time.Now()
	base := entry{ActionID: actionID, OutputID: output1, Size: 1, CompressedSize: 1, CreatedAt: now, AccessedAt: now}

	if err := st.q.upsertEntry(ctx, base, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.q.updateBlobType(ctx, output1, blobTypeGoPackageArchive, blobClassifierVersion); err != nil {
		t.Fatal(err)
	}

	// Re-putting the action with a different output must drop stale classifications.
	changed := base
	changed.OutputID = output2
	if err := st.q.upsertEntry(ctx, changed, nil); err != nil {
		t.Fatal(err)
	}
	var blobType optionalInt64
	queryDB(t, st.db, `SELECT blob_type FROM entries WHERE action_id = ?`, []any{actionID}, &blobType)
	if blobType.ok {
		t.Fatalf("blob_type = %d after output change, want NULL", blobType.value)
	}

	// Re-putting with the same output preserves cached classifications.
	if err := st.q.updateBlobType(ctx, output2, blobTypeGoSource, blobClassifierVersion); err != nil {
		t.Fatal(err)
	}
	if err := st.q.upsertEntry(ctx, changed, nil); err != nil {
		t.Fatal(err)
	}
	queryDB(t, st.db, `SELECT blob_type FROM entries WHERE action_id = ?`, []any{actionID}, &blobType)
	if !blobType.ok || blobTypeKind(blobType.value) != blobTypeGoSource {
		t.Fatalf("blob_type = %v after same-output re-put, want %d", blobType, blobTypeGoSource)
	}
}

func TestMigrateSchemaAddsClassificationColumns(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	conn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite|sqlite.OpenCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck

	if err := sqlitex.ExecuteScript(conn, `
CREATE TABLE entries (
	action_id TEXT PRIMARY KEY,
	output_id TEXT NOT NULL,
	size INTEGER NOT NULL,
	compressed_size INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	accessed_at INTEGER NOT NULL
);
CREATE INDEX entries_output_id ON entries(output_id)`, nil); err != nil {
		t.Fatal(err)
	}

	for _, col := range []string{"blob_type", "blob_type_version"} {
		if has, err := entriesHasColumn(conn, col); err != nil {
			t.Fatal(err)
		} else if has {
			t.Fatalf("column %s present before migration", col)
		}
	}

	if err := migrateSchema(conn); err != nil {
		t.Fatal(err)
	}
	// Idempotent: running again must not fail.
	if err := migrateSchema(conn); err != nil {
		t.Fatal(err)
	}

	for _, col := range []string{"blob_type", "blob_type_version"} {
		if has, err := entriesHasColumn(conn, col); err != nil {
			t.Fatal(err)
		} else if !has {
			t.Fatalf("column %s missing after migration", col)
		}
	}

	// The plain output_id index is replaced by the covering index.
	if has, err := indexExists(conn, "entries_output_cover"); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Fatal("entries_output_cover missing after migration")
	}
	if current, err := statusCoverIndexCurrent(conn); err != nil {
		t.Fatal(err)
	} else if !current {
		t.Fatal("entries_output_cover does not cover status classifications")
	}
	if has, err := indexExists(conn, "entries_output_id"); err != nil {
		t.Fatal(err)
	} else if has {
		t.Fatal("entries_output_id present after migration")
	}
}

func TestReopenCacheReordersOutputCoverIndex(t *testing.T) {
	t.Parallel()

	cfg := config{dir: t.TempDir()}
	st, err := newStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// A resource-fork body has a row size above 0, so the counter bound below can fail.
	body := forkBody(41)
	actionID := sha256Sum([]byte("existing action"))
	outputID := sha256Sum(body)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		st.close()
		t.Fatal(err)
	}
	original, found, err := st.lookupEntry(hexOf(actionID))
	if err != nil || !found {
		st.close()
		t.Fatalf("lookup original entry: found=%t, err=%v", found, err)
	}
	if original.CompressedSize <= 0 {
		st.close()
		t.Fatalf("original compressed size = %d, want above 0", original.CompressedSize)
	}
	execDB(t, st.db, `UPDATE entries SET blob_type = 7, blob_type_version = 3`)
	if err := st.db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		return sqlitex.ExecuteScript(conn, `
DROP INDEX entries_output_cover;
CREATE INDEX entries_output_cover ON entries(output_id, size, compressed_size, blob_type, blob_type_version);
`, nil)
	}); err != nil {
		st.close()
		t.Fatal(err)
	}
	st.close()

	for attempt := range 2 {
		st, err := newStore(cfg)
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer st.close()
			ent, found, err := st.lookupEntry(hexOf(actionID))
			if err != nil || !found {
				t.Fatalf("lookup migrated entry: found=%t, err=%v", found, err)
			}
			if ent != original {
				t.Fatalf("migrated entry = %+v, want %+v", ent, original)
			}
			// The first walk after open sets the counter to the stored total,
			// which includes the catalog files.
			if got := compressedSizeForTest(t, st); got < original.CompressedSize {
				t.Fatalf("compressed size = %d, want at least %d", got, original.CompressedSize)
			}
			var version, blobType, blobVersion int64
			queryDB(t, st.db, `PRAGMA user_version`, nil, &version)
			if version != cacheSchemaVersion {
				t.Fatalf("catalog version = %d, want existing version %d", version, cacheSchemaVersion)
			}
			queryDB(t, st.db, `SELECT blob_type, blob_type_version FROM entries`, nil,
				&blobType, &blobVersion)
			if blobType != 7 || blobVersion != 3 {
				t.Fatalf("classifications changed: %d/%d", blobType, blobVersion)
			}
			var columns string
			queryDB(t, st.db, `SELECT group_concat(name, ',') FROM (SELECT name FROM pragma_index_info('entries_output_cover') ORDER BY seqno)`, nil, &columns)
			if want := "output_id,compressed_size,size,blob_type,blob_type_version"; columns != want {
				t.Fatalf("covering index columns = %q, want %q", columns, want)
			}
			if attempt == 0 {
				return
			}
			res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
			if err != nil {
				t.Fatal(err)
			}
			if res.Miss || !bytes.Equal(res.OutputID, outputID) {
				t.Fatalf("get migrated entry = %+v", res)
			}
			got, err := os.ReadFile(res.DiskPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("migrated body = %q, want %q", got, body)
			}
		}()
	}
}

func indexExists(conn *sqlite.Conn, name string) (bool, error) {
	count, err := queryInt64(conn, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, []any{name})
	return count > 0, err
}

func assertBlobKind(t *testing.T, statuses []blobTypeStatus, kind blobTypeKind, count int64) {
	t.Helper()
	for _, status := range statuses {
		if status.kind == kind {
			if status.count != count {
				t.Fatalf("kind %s count = %d, want %d", kind.label(), status.count, count)
			}
			return
		}
	}
	t.Fatalf("kind %s not found in statuses", kind.label())
}

func execCatalog(t *testing.T, dbPath, query string, args ...any) {
	t.Helper()
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	execDB(t, db, query, args...)
}

func queryCatalog(t *testing.T, dbPath, query string, args []any, dest ...any) {
	t.Helper()
	db, err := openExistingDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	queryDB(t, db, query, args, dest...)
}

func TestRunReportsInitialWriteError(t *testing.T) {
	t.Parallel()

	if err := run([]string{"-dir", t.TempDir()}, strings.NewReader(""), errWriter{}); err == nil {
		t.Fatal("run accepted failing stdout")
	}
}

func TestStdoutIsTerminal(t *testing.T) {
	t.Parallel()

	// The go command connects the helper's stdout to a pipe and tests use
	// in-memory writers; none of these must be detected as a terminal, so the
	// protocol handshake is still emitted rather than usage.
	if stdoutIsTerminal(&bytes.Buffer{}) {
		t.Fatal("bytes.Buffer reported as terminal")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close() //nolint:errcheck
	defer w.Close() //nolint:errcheck
	if stdoutIsTerminal(w) {
		t.Fatal("os.Pipe reported as terminal")
	}
}

func TestRunProtocolIgnoresTerminalCheckForPipes(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	stdin := strings.NewReader(`{"ID":1,"Command":"close"}` + "\n")
	if err := run([]string{"-dir", t.TempDir()}, stdin, &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), `"KnownCommands"`) {
		t.Fatalf("protocol handshake missing; got %q", stdout.String())
	}
}

func TestRunReportsUsageErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		mode runMode
	}{
		{"unknown flag", []string{"-bogus"}, runModeProtocol},
		{"missing flag value", []string{"-dir"}, runModeProtocol},
		{"unknown subcommand", []string{"bogus"}, runModeProtocol},
		{"status unknown flag", []string{"status", "-bogus"}, runModeStatus},
		{"clean stray argument", []string{"clean", "extra"}, runModeClean},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stdout bytes.Buffer
			err := run(tc.args, strings.NewReader(""), &stdout)
			var ue *usageError
			if !errors.As(err, &ue) {
				t.Fatalf("error = %v, want usageError", err)
			}
			if ue.mode != tc.mode {
				t.Fatalf("mode = %q, want %q", ue.mode, tc.mode)
			}
			if stdout.Len() != 0 {
				t.Fatalf("usage errors must not write to stdout; got %q", stdout.String())
			}
		})
	}
}

func TestRunValueErrorIsNotUsageError(t *testing.T) {
	t.Parallel()

	err := run([]string{"-dir", t.TempDir(), "-max-size", "wat"}, strings.NewReader(""), &bytes.Buffer{})
	if err == nil {
		t.Fatal("run accepted bad max-size")
	}
	var ue *usageError
	if errors.As(err, &ue) {
		t.Fatalf("value error classified as usageError: %v", err)
	}
}

func TestRunWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	cpuProfile := filepath.Join(dir, "cpu.pprof")
	memProfile := filepath.Join(dir, "mem.pprof")

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{
		"-dir", filepath.Join(dir, "cache"),
		"-max-size", "0",
		"-cpuprofile", cpuProfile,
		"-memprofile", memProfile,
	}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	assertNonEmptyFile(t, cpuProfile)
	assertNonEmptyFile(t, memProfile)
}

func TestRunProtocolConcurrentGets(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{16}, 32)
	outputID := bytes.Repeat([]byte{17}, 32)
	body := []byte("concurrent protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	for id := int64(2); id < 42; id++ {
		writeJSON(t, &stdin, request{
			ID:       id,
			Command:  cmdGet,
			ActionID: actionID,
		})
	}

	writeJSON(t, &stdin, request{ID: 42, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" {
		t.Fatalf("put response = %+v", putRes)
	}

	seen := map[int64]bool{}
	for range 41 {
		var res response
		if err := dec.Decode(&res); err != nil {
			t.Fatal(err)
		}
		if res.Err != "" {
			t.Fatalf("response %d has error: %s", res.ID, res.Err)
		}
		seen[res.ID] = true
		if res.ID >= 2 && res.ID < 42 && (res.Miss || !bytes.Equal(res.OutputID, outputID)) {
			t.Fatalf("get response = %+v", res)
		}
	}

	for id := int64(2); id <= 42; id++ {
		if !seen[id] {
			t.Fatalf("missing response ID %d", id)
		}
	}
}

func TestRunReturnsProtocolReadError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader("{bad json}\n"), &stdout)
	if err == nil {
		t.Fatal("run succeeded with bad JSON request")
	}
}

func TestRunUnknownCommandResponse(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{ID: 1, Command: command("wat")})
	writeJSON(t, &stdin, request{ID: 2, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var unknown response
	if err := dec.Decode(&unknown); err != nil {
		t.Fatal(err)
	}
	if unknown.ID != 1 || !strings.Contains(unknown.Err, "unknown command") {
		t.Fatalf("unknown command response = %+v", unknown)
	}
}

func TestProtocolHelpersRejectInvalidBodies(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), -1); err == nil {
		t.Fatal("bodyReader accepted negative size")
	}
	if _, err := bodyReader(bufio.NewReader(strings.NewReader("null")), 1); err == nil {
		t.Fatal("bodyReader accepted non-string body")
	}
	r, err := bodyReader(bufio.NewReader(strings.NewReader(`"bad\n"`)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("bodyReader accepted escaped string body")
	}
}

func TestJSONStringReaderSmallReads(t *testing.T) {
	t.Parallel()

	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(`"abcdef"`), 3))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for {
		n, err := raw.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "abcdef" {
		t.Fatalf("read = %q, want abcdef", got)
	}
}

func TestReadRequestSkipsBlankLinesAndReportsEOF(t *testing.T) {
	t.Parallel()

	br := bufio.NewReader(strings.NewReader("\n \n"))
	if _, err := readRequest(br); !errors.Is(err, io.EOF) {
		t.Fatalf("readRequest err = %v, want EOF", err)
	}
}

func TestBodyReaderZeroSize(t *testing.T) {
	t.Parallel()

	r, err := bodyReader(bufio.NewReader(strings.NewReader("")), 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestBodyReaderReportsEOFBeforeString(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), 1); !errors.Is(err, io.EOF) {
		t.Fatalf("bodyReader err = %v, want EOF", err)
	}
}

func TestJSONStringReaderLargeStringAndZeroRead(t *testing.T) {
	t.Parallel()

	want := strings.Repeat("a", 5000)
	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(strconvQuote(want)), 16))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := raw.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero Read = %d, %v; want 0, nil", n, err)
	}
	got, err := io.ReadAll(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("read len = %d, want %d", len(got), len(want))
	}
}

func TestResponseWriterKeepsFirstError(t *testing.T) {
	t.Parallel()

	rw := &responseWriter{enc: json.NewEncoder(errWriter{})}
	if err := rw.write(response{ID: 1}); err == nil {
		t.Fatal("write succeeded")
	}
	if err := rw.write(response{ID: 2}); err == nil {
		t.Fatal("second write succeeded")
	}
	if err := rw.err(); err == nil {
		t.Fatal("err returned nil")
	}
}

func TestParseFlagsLoadsDefaultConfigFile(t *testing.T) {
	setUserDirEnv(t)

	configPath, _ := defaultConfigPath()
	cacheDir := filepath.Join(t.TempDir(), "configured-cache")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o777); err != nil {
		t.Fatal(err)
	}
	configJSON := `{
		"cacheDir": ` + strconvQuote(cacheDir) + `,
		"maxSize": "123MiB",
		"maxAge": "3d",
		"verbose": true
	}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 123<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 123<<20)
	}
	if cfg.maxAge != 3*24*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 3*24*time.Hour)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

func TestParseFlagsUsesUserCacheDirDefault(t *testing.T) {
	setUserDirEnv(t)

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(userCacheDir, "gocachez")
	if cfg.dir != want {
		t.Fatalf("dir = %q, want %q", cfg.dir, want)
	}
	if cfg.maxAge != defaultMaxAge {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, defaultMaxAge)
	}
}

func TestParseFlagsRequiresExplicitConfig(t *testing.T) {
	if _, err := parseFlags([]string{"-config", filepath.Join(t.TempDir(), "missing.json")}); err == nil {
		t.Fatal("parseFlags succeeded with a missing explicit config")
	}
}

func TestParseFlagsOverrideConfigFile(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	configCacheDir := filepath.Join(configDir, "config-cache")
	flagCacheDir := filepath.Join(configDir, "flag-cache")
	configJSON := `{"cacheDir": ` + strconvQuote(configCacheDir) + `, "maxSize": "10MiB", "maxAge": "10d"}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags([]string{"-config", configPath, "-dir", flagCacheDir, "-max-size", "1MiB", "-max-age", "2d"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != flagCacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, flagCacheDir)
	}
	if cfg.maxSize != 1<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 1<<20)
	}
	if cfg.maxAge != 2*24*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 2*24*time.Hour)
	}
}

func TestParseFlagsUsesEnvironmentOverrides(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("GOCACHEZ_DIR", cacheDir)
	t.Setenv("GOCACHEZ_MAX_SIZE", "7MiB")
	t.Setenv("GOCACHEZ_MAX_AGE", "36h")
	t.Setenv("GOCACHEZ_VERBOSE", "true")
	t.Setenv("GOCACHEZ_CONFIG", "")

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 7<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 7<<20)
	}
	if cfg.maxAge != 36*time.Hour {
		t.Fatalf("maxAge = %v, want %v", cfg.maxAge, 36*time.Hour)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

func TestDefaultConfigPathUsesEnvironment(t *testing.T) {
	t.Setenv("GOCACHEZ_CONFIG", filepath.Join(t.TempDir(), "config.json"))

	path, required := defaultConfigPath()
	if path != os.Getenv("GOCACHEZ_CONFIG") || !required {
		t.Fatalf("defaultConfigPath = %q, %v; want env path required", path, required)
	}
}

func TestParseFlagsRejectsInvalidInputs(t *testing.T) {
	if _, err := parseFlags([]string{"-bad"}); err == nil {
		t.Fatal("parseFlags accepted unknown flag")
	}
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-max-size", "bad"}); err == nil {
		t.Fatal("parseFlags accepted bad max size")
	}
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-max-age", "bad"}); err == nil {
		t.Fatal("parseFlags accepted bad max age")
	}

	t.Setenv("GOCACHEZ_MAX_SIZE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_MAX_SIZE")
	}
	t.Setenv("GOCACHEZ_MAX_SIZE", "")
	t.Setenv("GOCACHEZ_MAX_AGE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_MAX_AGE")
	}
	t.Setenv("GOCACHEZ_MAX_AGE", "")
	t.Setenv("GOCACHEZ_VERBOSE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_VERBOSE")
	}
}

func TestApplyConfigFileRejectsInvalidJSONAndSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad-json.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o666); err != nil {
		t.Fatal(err)
	}
	var cfg config
	if err := applyConfigFile(&cfg, badJSON, true); err == nil {
		t.Fatal("applyConfigFile accepted bad JSON")
	}

	badSize := filepath.Join(dir, "bad-size.json")
	if err := os.WriteFile(badSize, []byte(`{"maxSize":"wat"}`), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigFile(&cfg, badSize, true); err == nil {
		t.Fatal("applyConfigFile accepted bad maxSize")
	}
}

func TestParseSize(t *testing.T) {
	t.Parallel()

	tests := map[string]int64{
		"0":      0,
		"42":     42,
		"1KiB":   1 << 10,
		"1.5MiB": 1<<20 + 1<<19,
		"2g":     2 << 30,
	}
	for input, want := range tests {
		got, err := parseSize(input)
		if err != nil {
			t.Fatalf("parseSize(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseSizeErrors(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "MiB", "1bad", "-1", "."} {
		if _, err := parseSize(input); err == nil {
			t.Fatalf("parseSize(%q) succeeded", input)
		}
	}
}

func TestParseAge(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]time.Duration{
		"0":    0,
		"5d":   5 * 24 * time.Hour,
		"1.5d": 36 * time.Hour,
		"36h":  36 * time.Hour,
		"90m":  90 * time.Minute,
	} {
		got, err := parseAge(input)
		if err != nil {
			t.Fatalf("parseAge(%q) error: %v", input, err)
		}
		if got != want {
			t.Fatalf("parseAge(%q) = %v, want %v", input, got, want)
		}
	}

	for _, input := range []string{"", "d", "-5d", "-1h", "wat", "5x"} {
		if _, err := parseAge(input); err == nil {
			t.Fatalf("parseAge(%q) succeeded", input)
		}
	}
}

func TestFormatSize(t *testing.T) {
	t.Parallel()

	tests := map[int64]string{
		0:       "0B",
		42:      "42B",
		1024:    "1.0KiB",
		1536:    "1.5KiB",
		1 << 20: "1.0MiB",
		1 << 30: "1.0GiB",
	}
	for input, want := range tests {
		if got := formatSize(input); got != want {
			t.Fatalf("formatSize(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestFormatSavings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		uncompressed int64
		compressed   int64
		want         string
	}{
		"empty": {
			uncompressed: 0,
			compressed:   0,
			want:         "0B (0.0%)",
		},
		"saved": {
			uncompressed: 100,
			compressed:   25,
			want:         "75B (75.0%)",
		},
		"grew": {
			uncompressed: 100,
			compressed:   125,
			want:         "-25B (-25.0%)",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatSavings(tc.uncompressed, tc.compressed); got != tc.want {
				t.Fatalf("formatSavings(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.want)
			}
		})
	}
}

func TestFormatSavingsParts(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		uncompressed int64
		compressed   int64
		wantAmount   string
		wantPercent  string
	}{
		"empty": {
			uncompressed: 0,
			compressed:   13,
			wantAmount:   "0B",
			wantPercent:  "0.0%",
		},
		"saved": {
			uncompressed: 100,
			compressed:   25,
			wantAmount:   "75B",
			wantPercent:  "75.0%",
		},
		"grew": {
			uncompressed: 100,
			compressed:   125,
			wantAmount:   "-25B",
			wantPercent:  "-25.0%",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatSavingsAmount(tc.uncompressed, tc.compressed); got != tc.wantAmount {
				t.Fatalf("formatSavingsAmount(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.wantAmount)
			}
			if got := formatSavingsPercent(tc.uncompressed, tc.compressed); got != tc.wantPercent {
				t.Fatalf("formatSavingsPercent(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.wantPercent)
			}
		})
	}
}

func TestFormatAge(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		age  time.Duration
		want string
	}{
		"negative": {-time.Second, "<1m"},
		"seconds":  {30 * time.Second, "<1m"},
		"minutes":  {42 * time.Minute, "42m"},
		"hours":    {3*time.Hour + 12*time.Minute, "3h 12m"},
		"days":     {5*24*time.Hour + 4*time.Hour, "5d 4h"},
		"years":    {2*365*24*time.Hour + 3*24*time.Hour, "2y 3d"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatAge(tc.age); got != tc.want {
				t.Fatalf("formatAge(%v) = %q, want %q", tc.age, got, tc.want)
			}
		})
	}
}

func TestFormatMaxUsage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		size    int64
		maxSize int64
		want    string
	}{
		"disabled": {size: 10, maxSize: 0, want: "disabled"},
		"empty":    {size: 0, maxSize: 20 << 30, want: "0B / 20.0GiB (0.0%, 20.0GiB remaining)"},
		"half":     {size: 10 << 30, maxSize: 20 << 30, want: "10.0GiB / 20.0GiB (50.0%, 10.0GiB remaining)"},
		"over":     {size: 25 << 30, maxSize: 20 << 30, want: "25.0GiB / 20.0GiB (125.0%, -5.0GiB remaining)"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatMaxUsage(tc.size, tc.maxSize); got != tc.want {
				t.Fatalf("formatMaxUsage(%d, %d) = %q, want %q", tc.size, tc.maxSize, got, tc.want)
			}
		})
	}
}

func TestFormatMaxAge(t *testing.T) {
	t.Parallel()

	for input, want := range map[time.Duration]string{
		0:                  "disabled",
		-time.Hour:         "disabled",
		5 * 24 * time.Hour: "5d",
		36 * time.Hour:     "1d 12h",
		90 * time.Minute:   "1h 30m",
	} {
		if got := formatMaxAge(input); got != want {
			t.Fatalf("formatMaxAge(%v) = %q, want %q", input, got, want)
		}
	}
}

func encodedBody(body []byte) *bytes.Buffer {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(base64.StdEncoding.EncodeToString(body)); err != nil {
		panic(err)
	}
	return &buf
}

func writeJSON(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		t.Fatal(err)
	}
}

func hexOf(id []byte) string {
	return hex.EncodeToString(id)
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func goArchive(pkgdef, object []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(archiveMagic)
	writeArchiveMember(&buf, pkgdefName, pkgdef)
	writeArchiveMember(&buf, "_go_.o", object)
	return buf.Bytes()
}

func indexedExportData(t *testing.T) []byte {
	t.Helper()

	pkg := types.NewPackage("example.com/p", "p")
	pkg.MarkComplete()
	var buf bytes.Buffer
	if err := gcexportdata.Write(&buf, token.NewFileSet(), pkg); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func goPkgdef(exportData []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("go object fake\n")
	buf.WriteString("$$B\n")
	buf.Write(exportData)
	buf.WriteString("\n$$\n")
	return buf.Bytes()
}

func writeArchiveMember(buf *bytes.Buffer, name string, body []byte) {
	fmt.Fprintf(buf, "%-16s%-12d%-6d%-6d%-8o%-10d`\n", name, 0, 0, 0, 0o644, len(body))
	buf.Write(body)
	if len(body)%2 != 0 {
		buf.WriteByte('\n')
	}
}

func execDB(t *testing.T, db *sqliteDB, query string, args ...any) {
	t.Helper()
	err := db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		return execute(conn, query, args...)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func addSizedCacheEntries(t *testing.T, st *store, now time.Time, count int, size int64) []string {
	t.Helper()

	outputIDs := make([]string, count)
	for i := range count {
		actionID := fmt.Sprintf("%064x", i+1)
		outputID := fmt.Sprintf("%064x", i+count+1)
		outputIDs[i] = outputID
		if err := os.MkdirAll(st.storedDir(outputID), 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(st.storedPath(outputID), []byte("blob"), 0o666); err != nil {
			t.Fatal(err)
		}
		if err := st.upsertEntry(entry{
			ActionID:       actionID,
			OutputID:       outputID,
			Size:           size,
			CompressedSize: size,
			CreatedAt:      now.Add(time.Duration(i) * time.Second),
			AccessedAt:     now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return outputIDs
}

func compressedSizeForTest(t *testing.T, st *store) int64 {
	t.Helper()
	size, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	return size
}

func queryDB(t *testing.T, db *sqliteDB, query string, args []any, dest ...any) {
	t.Helper()
	found := false
	err := db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn, query, &sqlitex.ExecOptions{
			Args: args,
			ResultFunc: func(stmt *sqlite.Stmt) error {
				found = true
				for i, target := range dest {
					switch target := target.(type) {
					case *int:
						*target = int(stmt.ColumnInt64(i))
					case *int64:
						*target = stmt.ColumnInt64(i)
					case *string:
						*target = stmt.ColumnText(i)
					case *optionalInt64:
						target.ok = !stmt.ColumnIsNull(i)
						if target.ok {
							target.value = stmt.ColumnInt64(i)
						}
					default:
						return fmt.Errorf("unsupported query destination %T", target)
					}
				}
				return nil
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("query returned no rows")
	}
}

func countRows(t *testing.T, db *sqliteDB, query string, args ...any) int {
	t.Helper()
	var count int
	queryDB(t, db, query, args, &count)
	return count
}

func assertNonEmptyFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("%s is empty", path)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("output missing %q:\n%s", want, got)
	}
}

func setUserDirEnv(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	home := filepath.Join(root, "home")
	cacheDir := filepath.Join(root, "cache")
	configDir := filepath.Join(root, "config")
	for _, dir := range []string{home, cacheDir, configDir} {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("LOCALAPPDATA", cacheDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("GOCACHEZ_CONFIG", "")
}

func abandonStore(t *testing.T, st *store) {
	t.Helper()
	if err := st.runLock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := st.runLock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

// removeFileReader removes path on its second Read. The put's first Read opens
// the JSON string, so the second one comes after put has checked the stored
// file and while it counts the body.
type removeFileReader struct {
	reader  io.Reader
	path    string
	reads   int
	removed bool
}

func (r *removeFileReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 2 {
		r.removed = true
		if err := os.Remove(r.path); err != nil {
			return 0, err
		}
	}
	return r.reader.Read(p)
}

func strconvQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// writeOrphanOutput writes a stored file that no catalog row names and ages its
// mtime past the one-hour floor, so the walk may remove it.
func writeOrphanOutput(t *testing.T, st *store, outputID string) string {
	t.Helper()
	path := st.storedPath(outputID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	return path
}
