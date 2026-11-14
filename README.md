# gocachez

`gocachez` is a [`GOCACHEPROG`](https://pkg.go.dev/cmd/go/internal/cacheprog)
helper for the Go build cache. It stores each cache artifact as one
file-system-compressed file (APFS, LZFSE), hands that file to the Go command as
is, and evicts old entries automatically. It runs on macOS only.

This project was inspired by the discussion on
[`golang/go#76337`](https://github.com/golang/go/issues/76337).

The two tables below are the upstream project's measurements of its
zstd-compressed design. They do not describe this fork.

Go `std`:

| Mode             |       Cold time |       Warm time | Disk usage |
| ---------------- | --------------: | --------------: | ---------: |
| No `GOCACHEPROG` |          36.31s |           4.06s |     1.60GB |
| `gocachez`       |          40.97s |           5.65s |      403MB |
|                  | +4.66s (+12.8%) | +1.59s (+39.1%) |     -74.8% |

[`typescript-go`](https://github.com/microsoft/typescript-go):

| Mode             |      Cold time |       Warm time | Disk usage |
| ---------------- | -------------: | --------------: | ---------: |
| No `GOCACHEPROG` |         89.20s |           4.76s |     4.95GB |
| `gocachez`       |         96.84s |           6.97s |      453MB |
|                  | +7.64s (+8.6%) | +2.21s (+46.4%) |     -90.9% |

## Installation

Requires macOS, cgo, and Go 1.25 or newer.

`GOCACHE` must be a path prefix of the stored files, which sit under
`<cache directory>/v2/outputs/`. For example, set `GOCACHE` to the cache
directory. The Go command copies a cached file only when its path is under
`GOCACHE`, and otherwise moves it out of the store.

```console
$ go install github.com/darthshadow/gocachez@latest
$ go env -w GOCACHEPROG=gocachez
```

Undo:

```console
$ go env -u GOCACHEPROG
```

## Commands

Show help:

```console
$ gocachez -h
```

Run as a `GOCACHEPROG` helper:

```console
$ gocachez
```

Remove inactive cache state:

```console
$ gocachez clean
```

`clean` removes the stored files and catalog state that no active `gocachez`
process is using. Nothing is removed while a run is active.

Show cache state:

```console
$ gocachez status
```

`status` reports the current cache configuration and state.

## How it works

`gocachez` implements the Go command's external cache protocol over stdin and
stdout. The Go command sends `put` requests when it wants to store an artifact
and `get` requests when it wants to retrieve one.

On `put`, `gocachez`:

1. streams the artifact body in 64 KiB chunks into LZFSE (libcompression),
2. writes the compressed chunks as the file's `com.apple.decmpfs` and resource
   fork attributes into a temp file, and sets `UF_COMPRESSED`,
3. makes the file read-only (0444) and publishes it at
   `v2/outputs/<first two hex digits>/<output ID>`, and
4. returns that path to the Go command as `DiskPath`.

The file system decompresses the file when the Go command reads it. No plain
copy is written. A body of 2 GiB or more is rejected.

On `get`, `gocachez` looks up the action ID in SQLite. On a hit, it returns the
stored path as `DiskPath` and writes nothing. The Go command checks the file
itself and treats a missing or wrong-sized file as a miss.

The stored file is the only copy, so a path that `gocachez` returned can outlive
the helper: `go list -export` reports an `Export` path that tools such as
`go/packages` and `golangci-lint` open after the Go command has closed its
`GOCACHEPROG` helper. Two rules protect those paths:

- No file is deleted while any `gocachez` process is running.
- No output is deleted within 1 hour after its last use.

Each run holds an OS file lock. If `gocachez` exits abnormally, a later run
reclaims the abandoned run directory once it can acquire that lock.

Once an hour, with no run active, `gocachez` removes temp files and files with
no catalog entry that are older than 1 hour, then sets its size counter to the
bytes on disk (`st_blocks`) under `v2/`.

## Configuration

By default, `gocachez` reads its config from:

```text
os.UserConfigDir()/gocachez/config.json
```

and stores cache data in:

```text
os.UserCacheDir()/gocachez
```

See [`os.UserConfigDir`](https://pkg.go.dev/os#UserConfigDir) and
[`os.UserCacheDir`](https://pkg.go.dev/os#UserCacheDir) for the
platform-specific base directories.

Example config:

```json
{
    "cacheDir": "/path/to/gocachez",
    "maxSize": "20GiB",
    "maxAge": "5d",
    "verbose": false
}
```

Config can also be selected explicitly:

```console
$ go env -w GOCACHEPROG="gocachez -config /path/to/config.json"
```

Supported options:

**Cache directory**

- Config: `cacheDir`
- Environment: `GOCACHEZ_DIR`
- Flag: `-dir`
- Default: `os.UserCacheDir()/gocachez`

**Maximum cache size on disk**

- Config: `maxSize`
- Environment: `GOCACHEZ_MAX_SIZE`
- Flag: `-max-size`
- Default: `20GiB`; `0` disables size-based pruning.
  The limit counts the bytes on disk under `v2/`, catalog included. The total
  can pass it while a run is active or an output is inside the 1 hour window.
  The next prune then trims it to 90 percent of the limit.

**Maximum age of unused entries**

- Config: `maxAge`
- Environment: `GOCACHEZ_MAX_AGE`
- Flag: `-max-age`
- Default: `5d`, matching `cmd/go`'s `GOCACHE`; `0` disables age-based pruning.
  Accepts a duration such as `5d`, `36h`, or `90m`.

Age-based pruning is independent of `maxSize`: entries that
have not been used within `maxAge` are trimmed even while the cache is under its
size limit.

**Verbose maintenance logs**

- Config: `verbose`
- Environment: `GOCACHEZ_VERBOSE`
- Flag: `-v`
- Default: `false`

**Config file path**

- Environment: `GOCACHEZ_CONFIG`
- Flag: `-config`
- Default: `os.UserConfigDir()/gocachez/config.json`
- Missing file is an error when explicitly set.
