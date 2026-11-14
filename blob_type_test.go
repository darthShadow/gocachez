package main

import (
	"bytes"
	"testing"
)

func TestClassifyBlobDataRecognizesCCompilerID(t *testing.T) {
	t.Parallel()

	body := append([]byte("/usr/lib/ccache/bin/gcc\x00stat 1448536 1ed 2026-05-13 07:28:29 -0700 PDT false\n\x00"),
		bytes.Repeat([]byte{0x86}, 32)...)

	classification := classifyBlobData(body)
	if classification.kind != blobTypeCCompilerID {
		t.Fatalf("classification = %v, want %v", classification.kind, blobTypeCCompilerID)
	}
}

func TestClassifyBlobDataRecognizesGoTestOutput(t *testing.T) {
	t.Parallel()

	for _, body := range [][]byte{
		[]byte("\x16=== RUN   TestExample\n\x16--- PASS: TestExample (0.00s)\n\x16PASS\nok  \texample.com/pkg\t0.001s\n"),
		[]byte("testing: warning: no tests to run\n\x16PASS\nok  \texample.com/pkg\t0.001s\n"),
		[]byte("ok  \texample.com/pkg\t0.001s\n"),
	} {
		classification := classifyBlobData(body)
		if classification.kind != blobTypeGoTestOutput {
			t.Fatalf("classification = %v, want %v for %q", classification.kind, blobTypeGoTestOutput, body)
		}
	}
}

func TestClassifyBlobDataRecognizesGoCacheTextFormats(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body []byte
		want blobTypeKind
	}{
		{
			name: "test input log",
			body: []byte("# test log\ngetenv GODEBUG\nopen /tmp/example\n"),
			want: blobTypeGoTestInputLog,
		},
		{
			name: "coverage profile",
			body: []byte("mode: atomic\nexample.go:1.2,3.4 1 1\n"),
			want: blobTypeGoCoverageProfile,
		},
		{
			name: "tool output",
			body: []byte("# example.com/pkg\ninternal/pkg/pkg.go:10:6: can inline f\n"),
			want: blobTypeGoToolOutput,
		},
		{
			name: "source file list",
			body: []byte("./a.go\n./b.s\n_cgo_export.c\n"),
			want: blobTypeGoSourceFileList,
		},
		{
			name: "source file list with live path",
			body: []byte(".//tmp/gocachez/v1/live/run-123/abcdef-456\n"),
			want: blobTypeGoSourceFileList,
		},
		{
			name: "source file list truncated mid-line",
			body: append(bytes.Repeat([]byte("./a.go\n"), blobTypePrefixLimit/len("./a.go\n")),
				bytes.Repeat([]byte("x"), blobTypePrefixLimit%len("./a.go\n"))...),
			want: blobTypeGoSourceFileList,
		},
		{
			name: "tool flag probe",
			body: []byte("true"),
			want: blobTypeGoToolFlagProbe,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			classification := classifyBlobData(tc.body)
			if classification.kind != tc.want {
				t.Fatalf("classification = %v, want %v", classification.kind, tc.want)
			}
		})
	}
}

func TestClassifyBlobDataRecognizesIndexedExportData(t *testing.T) {
	t.Parallel()

	classification := classifyBlobData(indexedExportData(t))
	if classification.kind != blobTypeGoIndexedExportData {
		t.Fatalf("classification = %v, want %v", classification.kind, blobTypeGoIndexedExportData)
	}
}

func TestClassifyBlobDataRecognizesCoverageMetadata(t *testing.T) {
	t.Parallel()

	classification := classifyBlobData([]byte{0x00, 'c', 'v', 'm', 1, 0, 0, 0})
	if classification.kind != blobTypeGoCoverageMetadata {
		t.Fatalf("classification = %v, want %v", classification.kind, blobTypeGoCoverageMetadata)
	}
}
