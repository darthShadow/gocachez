package main

/*
#cgo LDFLAGS: -lcompression
#include <compression.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/xattr.h>

static size_t lzfse_scratch_size(void) {
	return compression_encode_scratch_buffer_size(COMPRESSION_LZFSE);
}

static size_t lzfse_encode(uint8_t *dst, size_t dst_size, const uint8_t *src, size_t src_size, void *scratch) {
	return compression_encode_buffer(dst, dst_size, src, src_size, scratch, COMPRESSION_LZFSE);
}

// resource_fork_write writes buf into the resource fork at position. The
// golang.org/x/sys/unix Fsetxattr has no position argument.
static int resource_fork_write(int fd, const void *buf, size_t size, uint32_t position) {
	return fsetxattr(fd, XATTR_RESOURCEFORK_NAME, buf, size, position, 0);
}
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// A stored file is a regular file that the file system compresses (APFS
// decmpfs, LZFSE). The writer builds the compressed form directly, so the
// helper never writes the plain bytes. The layout is the one macOS's own
// compressors produce:
//
//   - com.apple.decmpfs holds a 16-byte little-endian header: the magic, the
//     type (11 for LZFSE inline, 12 for LZFSE in the resource fork), and the
//     logical size. Type 11 appends one compressed chunk to the header.
//   - Type 12 keeps the chunks in com.apple.ResourceFork: n+1 uint32 offsets
//     from the start of the fork (chunk i starts at offset i; offset n is the
//     end), then the n chunks.
//   - The UF_COMPRESSED flag then makes the kernel read the file through these.
const (
	decmpfsMagic            uint32 = 0x636d7066 // "cmpf"
	decmpfsTypeInline       uint32 = 11
	decmpfsTypeResourceFork uint32 = 12
	decmpfsHeaderLen               = 16
	decmpfsXattr                   = "com.apple.decmpfs"

	// decmpfsMaxInlineValue is the largest com.apple.decmpfs value (3,802
	// bytes), so an inline chunk is at most 3,786 bytes.
	decmpfsMaxInlineValue = 3802
	decmpfsChunkSize      = 64 << 10
	// A chunk of random data grows by about 1.1 percent.
	decmpfsMaxChunk = decmpfsChunkSize + decmpfsChunkSize/8

	storedFileMode = 0o444
)

// setUFCompressed sets the flag that makes the kernel read the decmpfs data.
func setUFCompressed(fd int) error {
	return unix.Fchflags(fd, unix.UF_COMPRESSED)
}

// writeStoredTemp writes the body into a new temp file in dir and returns its
// path. The file is read-only and compressed (size 0 is an empty plain file).
// It checks that the body holds exactly size bytes. On any error it removes the
// temp file.
func (st *store) writeStoredTemp(dir, outputHex string, body io.Reader, size int64) (string, error) {
	f, err := os.CreateTemp(dir, outputHex+tempMarker+"*")
	if err != nil {
		return "", fmt.Errorf("create stored file: %w", err)
	}
	path := f.Name()
	if err := st.fillStoredTemp(f, body, size); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close stored file: %w", err)
	}
	return path, nil
}

// fillStoredTemp writes the body into f and leaves f read-only and flagged as
// compressed, with the logical size checked.
func (st *store) fillStoredTemp(f *os.File, body io.Reader, size int64) error {
	fd := int(f.Fd())
	if size > 0 {
		if err := writeDecmpfs(fd, body, size); err != nil {
			return err
		}
		// The flag comes last: the file must be writable while the attributes go
		// in, and the flag is what makes them the file's contents.
		if err := st.setCompressedFlag(fd); err != nil {
			return fmt.Errorf("flag stored file as compressed: %w", err)
		}
	}
	if err := unix.Fchmod(fd, storedFileMode); err != nil {
		return fmt.Errorf("make stored file read-only: %w", err)
	}
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		return fmt.Errorf("stat stored file: %w", err)
	}
	if info.Size != size {
		return fmt.Errorf("stored file size is %d, expected %d", info.Size, size)
	}
	if size > 0 && info.Flags&unix.UF_COMPRESSED == 0 {
		return errors.New("stored file is not flagged as compressed")
	}
	return nil
}

// writeDecmpfs reads exactly size bytes from body in chunks of decmpfsChunkSize
// and writes the file's compressed attributes. One chunk is in memory at a
// time.
func writeDecmpfs(fd int, body io.Reader, size int64) error {
	scratch := C.malloc(C.lzfse_scratch_size())
	if scratch == nil {
		return errors.New("allocate LZFSE scratch buffer")
	}
	defer C.free(scratch)

	chunks := (size + decmpfsChunkSize - 1) / decmpfsChunkSize
	offsets := make([]byte, 4*(chunks+1))
	src := make([]byte, decmpfsChunkSize)
	dst := make([]byte, decmpfsMaxChunk)
	position := uint64(len(offsets))
	var read int64
	for i := range chunks {
		want := min(decmpfsChunkSize, size-read)
		n, err := io.ReadFull(body, src[:want])
		read += int64(n)
		if err != nil {
			return bodyReadError(read, size, err)
		}
		encoded := int(C.lzfse_encode(
			(*C.uint8_t)(unsafe.Pointer(&dst[0])), C.size_t(len(dst)),
			(*C.uint8_t)(unsafe.Pointer(&src[0])), C.size_t(n), scratch))
		if encoded == 0 {
			return errors.New("LZFSE compression failed")
		}
		if chunks == 1 && decmpfsHeaderLen+encoded <= decmpfsMaxInlineValue {
			if err := requireBodyEnd(body, size); err != nil {
				return err
			}
			value := decmpfsHeader(decmpfsTypeInline, size)
			value = append(value, dst[:encoded]...)
			return setDecmpfs(fd, value)
		}
		if position+uint64(encoded) > math.MaxUint32 {
			return errors.New("compressed data does not fit the resource fork offsets")
		}
		binary.LittleEndian.PutUint32(offsets[4*i:], uint32(position))
		if err := writeResourceFork(fd, dst[:encoded], uint32(position)); err != nil {
			return err
		}
		position += uint64(encoded)
	}
	binary.LittleEndian.PutUint32(offsets[4*chunks:], uint32(position))
	if err := requireBodyEnd(body, size); err != nil {
		return err
	}
	if err := writeResourceFork(fd, offsets, 0); err != nil {
		return err
	}
	return setDecmpfs(fd, decmpfsHeader(decmpfsTypeResourceFork, size))
}

// requireBodyEnd checks that body has no byte after the size bytes already
// read, so a body longer than its declared size fails.
func requireBodyEnd(body io.Reader, size int64) error {
	var extra [1]byte
	n, err := io.ReadFull(body, extra[:])
	if n > 0 {
		return fmt.Errorf("put body size mismatch: got more than %d bytes", size)
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("read put body: %w", err)
	}
	return nil
}

func bodyReadError(read, size int64, err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("put body size mismatch: got %d bytes, expected %d", read, size)
	}
	return fmt.Errorf("read put body: %w", err)
}

func decmpfsHeader(kind uint32, size int64) []byte {
	header := make([]byte, decmpfsHeaderLen, decmpfsMaxInlineValue)
	binary.LittleEndian.PutUint32(header[0:], decmpfsMagic)
	binary.LittleEndian.PutUint32(header[4:], kind)
	binary.LittleEndian.PutUint64(header[8:], uint64(size))
	return header
}

func setDecmpfs(fd int, value []byte) error {
	if err := unix.Fsetxattr(fd, decmpfsXattr, value, 0); err != nil {
		return fmt.Errorf("write %s: %w", decmpfsXattr, err)
	}
	return nil
}

func writeResourceFork(fd int, data []byte, position uint32) error {
	if rc, err := C.resource_fork_write(C.int(fd), unsafe.Pointer(&data[0]), C.size_t(len(data)), C.uint32_t(position)); rc != 0 {
		if err == nil {
			err = errors.New("fsetxattr failed")
		}
		return fmt.Errorf("write resource fork at %d: %w", position, err)
	}
	return nil
}
