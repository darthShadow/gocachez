package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type cacheStatus struct {
	cacheDir         string
	maxSize          int64
	maxAge           time.Duration
	verbose          bool
	versionDir       string
	catalogExists    bool
	catalog          catalogStatus
	blobFiles        int64
	blobTypes        []blobTypeStatus
	activeLiveRuns   int64
	inactiveLiveRuns int64
}

type catalogStatus struct {
	entries           int64
	outputs           int64
	size              int64
	compressedSize    int64
	runs              int64
	oldestAccessedAt  time.Time
	hasOldestAccessed bool
}

func writeStatus(cfg config, w io.Writer) error {
	status, err := readStatus(cfg)
	if err != nil {
		return err
	}

	if err := writeTable(w, "Configuration", [][]string{
		{"Cache directory", status.cacheDir},
		{"Version directory", status.versionDir},
		{"Max size", formatBytes(status.maxSize)},
		{"Max age", formatMaxAge(status.maxAge)},
		{"Verbose", strconv.FormatBool(status.verbose)},
	}); err != nil {
		return err
	}
	catalogState := "missing"
	if status.catalogExists {
		catalogState = "present"
	}
	if err := writeTable(w, "Summary", [][]string{
		{"State", catalogState},
		{"Cached actions", formatInt(status.catalog.entries)},
		{"Cached outputs", formatInt(status.catalog.outputs)},
		{"Oldest cache entry", formatOldestAccess(status.catalog, time.Now())},
		{"Live runs", formatLiveRuns(status.activeLiveRuns, status.inactiveLiveRuns)},
	}); err != nil {
		return err
	}
	if err := writeTable(w, "Storage", [][]string{
		{"Original output size", formatBytes(status.catalog.size)},
		{"Compressed cache blobs", formatStorageSize(status.catalog.compressedSize, status.blobFiles)},
		{"Blob max usage", formatMaxUsage(status.catalog.compressedSize, status.maxSize)},
		{"Blob-only savings", formatSavings(status.catalog.size, status.catalog.compressedSize)},
	}); err != nil {
		return err
	}
	if err := writeBlobTypeStatus(w, status.blobTypes); err != nil {
		return err
	}
	return nil
}

func readStatus(cfg config) (cacheStatus, error) {
	versionDir, outputsDir, liveRoot, _ := cachePaths(cfg)
	status := cacheStatus{
		cacheDir:   cfg.dir,
		maxSize:    cfg.maxSize,
		maxAge:     cfg.maxAge,
		verbose:    cfg.verbose,
		versionDir: versionDir,
	}
	if _, err := os.Stat(versionDir); errors.Is(err, os.ErrNotExist) {
		return status, nil
	} else if err != nil {
		return cacheStatus{}, fmt.Errorf("stat cache version dir: %w", err)
	}

	dbPath := filepath.Join(versionDir, "cache.db")
	catalogExists, catalog, outputs, err := readCatalogStatus(dbPath)
	if err != nil {
		return cacheStatus{}, err
	}
	status.catalogExists = catalogExists
	status.catalog = catalog
	// The stored file count matches the number of cached outputs, so derive it
	// from the catalog instead of walking the outputs directory.
	status.blobFiles = status.catalog.outputs
	status.blobTypes = blobTypeStatuses(dbPath, outputsDir, outputs)
	status.activeLiveRuns, status.inactiveLiveRuns, err = readLiveStatus(liveRoot)
	if err != nil {
		return cacheStatus{}, err
	}
	return status, nil
}

// readCatalogStatus opens the catalog once and returns the aggregate status
// plus the per-output rows. It performs a single GROUP BY output_id scan
// (listOutputs) to derive the output count, sizes, and cached blob types, so
// callers can reuse outputs for the blob-type breakdown without rescanning.
func readCatalogStatus(dbPath string) (bool, catalogStatus, []catalogOutput, error) {
	if !regularFile(dbPath) {
		return false, catalogStatus{}, nil, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return false, catalogStatus{}, nil, err
	}
	defer db.Close() //nolint:errcheck

	var status catalogStatus
	err = db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		var err error
		status.entries, err = queryInt64(conn, `SELECT COUNT(*) FROM entries`, nil)
		if err != nil {
			return fmt.Errorf("count catalog entries: %w", err)
		}
		if err := sqlitex.Execute(conn, `SELECT MIN(accessed_at) FROM entries`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				if !stmt.ColumnIsNull(0) {
					status.oldestAccessedAt = millisTime(stmt.ColumnInt64(0))
					status.hasOldestAccessed = true
				}
				return nil
			},
		}); err != nil {
			return fmt.Errorf("find oldest catalog access: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, catalogStatus{}, nil, err
	}

	q := newCatalog(db)
	var hasBlobType bool
	err = db.withConn(context.Background(), func(conn *sqlite.Conn) error {
		var err error
		hasBlobType, err = entriesHasColumn(conn, "blob_type_version")
		return err
	})
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("inspect catalog schema: %w", err)
	}
	outputs, err := q.listOutputs(context.Background(), hasBlobType, blobClassifierVersion)
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("list catalog outputs: %w", err)
	}
	for _, output := range outputs {
		status.size += output.size
		status.compressedSize += output.compressedSize
	}
	status.outputs = int64(len(outputs))

	status.runs, err = q.countRuns(context.Background())
	if err != nil {
		return false, catalogStatus{}, nil, fmt.Errorf("count catalog runs: %w", err)
	}
	return true, status, outputs, nil
}

func writeBlobTypeStatus(w io.Writer, statuses []blobTypeStatus) error {
	if len(statuses) == 0 {
		return writeRightAlignedTable(w, "Compressed blob contents", [][]string{
			{"Type", "Files", "Original", "Stored", "Savings"},
			{"None", "0", formatBytes(0), formatBytes(0), formatAlignedSavings(0, 0, 2, 4)},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Original", "Stored", "Savings"},
	}
	amountWidth, percentWidth := savingsWidths(statuses)
	for _, status := range statuses {
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
			formatBytes(status.compressedSize),
			formatAlignedSavings(status.size, status.compressedSize, amountWidth, percentWidth),
		})
	}
	return writeRightAlignedTable(w, "Compressed blob contents", rows)
}

func savingsWidths(statuses []blobTypeStatus) (int, int) {
	amountWidth := len("0B")
	percentWidth := len("0.0%")
	for _, status := range statuses {
		amountWidth = max(amountWidth, len(formatSavingsAmount(status.size, status.compressedSize)))
		percentWidth = max(percentWidth, len(formatSavingsPercent(status.size, status.compressedSize)))
	}
	return amountWidth, percentWidth
}

func formatAlignedSavings(uncompressed, compressed int64, amountWidth, percentWidth int) string {
	return fmt.Sprintf("%*s (%*s)",
		amountWidth,
		formatSavingsAmount(uncompressed, compressed),
		percentWidth,
		formatSavingsPercent(uncompressed, compressed),
	)
}

func writeTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, false)
}

func writeRightAlignedTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, true)
}

func writeTableAligned(w io.Writer, title string, rows [][]string, rightAlignColumns bool) error {
	if _, err := fmt.Fprintf(w, "%s:\n", title); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	widths := tableWidths(rows)
	for _, row := range rows {
		if _, err := fmt.Fprint(w, "  "); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
		last := len(row) - 1
		for last > 0 && row[last] == "" {
			last--
		}
		for i, cell := range row[:last+1] {
			if i > 0 {
				if _, err := fmt.Fprint(w, "  "); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			padding := widths[i] - len(cell)
			if rightAlignColumns && i > 0 {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			if _, err := fmt.Fprint(w, cell); err != nil {
				return fmt.Errorf("write status: %w", err)
			}
			if i < last && (!rightAlignColumns || i == 0) {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func tableWidths(rows [][]string) []int {
	var widths []int
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], len(cell))
		}
	}
	return widths
}

func formatBytes(size int64) string {
	return formatSize(size)
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func formatStorageSize(size, files int64) string {
	return fmt.Sprintf("%s (%s)", formatBytes(size), formatFileCount(files))
}

func formatFileCount(files int64) string {
	if files == 1 {
		return "1 file"
	}
	return formatInt(files) + " files"
}

func formatLiveRuns(active, inactive int64) string {
	return fmt.Sprintf("%s active, %s inactive", formatInt(active), formatInt(inactive))
}

func formatMaxUsage(size, maxSize int64) string {
	if maxSize <= 0 {
		return "disabled"
	}
	percent := float64(size) / float64(maxSize) * 100
	remaining := maxSize - size
	return fmt.Sprintf("%s / %s (%.1f%%, %s remaining)", formatBytes(size), formatBytes(maxSize), percent, formatBytes(remaining))
}

func formatMaxAge(maxAge time.Duration) string {
	if maxAge <= 0 {
		return "disabled"
	}
	return formatAge(maxAge)
}

func formatOldestAccess(status catalogStatus, now time.Time) string {
	if !status.hasOldestAccessed {
		return "n/a"
	}
	return formatAge(now.Sub(status.oldestAccessedAt))
}

func formatAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	switch {
	case age < time.Minute:
		return "<1m"
	case age < time.Hour:
		return fmt.Sprintf("%dm", int(age/time.Minute))
	case age < 24*time.Hour:
		hours := int(age / time.Hour)
		minutes := int((age % time.Hour) / time.Minute)
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case age < 365*24*time.Hour:
		days := int(age / (24 * time.Hour))
		hours := int((age % (24 * time.Hour)) / time.Hour)
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, hours)
	default:
		years := int(age / (365 * 24 * time.Hour))
		days := int((age % (365 * 24 * time.Hour)) / (24 * time.Hour))
		if days == 0 {
			return fmt.Sprintf("%dy", years)
		}
		return fmt.Sprintf("%dy %dd", years, days)
	}
}

func readLiveStatus(liveRoot string) (int64, int64, error) {
	entries, err := os.ReadDir(liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read live dir: %w", err)
	}

	var active, inactive int64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runLock := flock.New(filepath.Join(liveRoot, entry.Name(), "run.lock"))
		locked, err := runLock.TryLock()
		if err != nil {
			_ = runLock.Close()
			return 0, 0, fmt.Errorf("try lock live run %s: %w", entry.Name(), err)
		}
		if !locked {
			_ = runLock.Close()
			active++
			continue
		}
		if err := runLock.Unlock(); err != nil {
			_ = runLock.Close()
			return 0, 0, fmt.Errorf("unlock live run: %w", err)
		}
		if err := runLock.Close(); err != nil {
			return 0, 0, fmt.Errorf("close live run lock: %w", err)
		}
		inactive++
	}
	return active, inactive, nil
}
