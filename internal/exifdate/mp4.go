package exifdate

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/spf13/afero"
)

// quickTimeEpoch is the reference for MP4/QuickTime timestamps: seconds since
// 1904-01-01 00:00:00 UTC.
var quickTimeEpoch = time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)

// mp4CreationDate reads the creation time from an MP4/MOV file's moov/mvhd atom.
// It walks only atom headers (seeking over payloads via ReadAt), so it stays
// cheap even for large videos and works whether moov is at the start or end.
func mp4CreationDate(fs afero.Fs, path string) (time.Time, bool) {
	f, err := fs.Open(path)
	if err != nil {
		return time.Time{}, false
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return time.Time{}, false
	}
	size := fi.Size()

	moovStart, moovEnd, ok := findAtom(f, 0, size, "moov")
	if !ok {
		return time.Time{}, false
	}
	mvhdStart, _, ok := findAtom(f, moovStart, moovEnd, "mvhd")
	if !ok {
		return time.Time{}, false
	}
	return readMvhd(f, mvhdStart)
}

// findAtom scans top-level atoms in [start, end) and returns the payload start
// and end offset of the first atom whose type matches want.
func findAtom(r io.ReaderAt, start, end int64, want string) (payloadStart, atomEnd int64, ok bool) {
	off := start
	hdr := make([]byte, 8)
	for off+8 <= end {
		if _, err := r.ReadAt(hdr, off); err != nil {
			return 0, 0, false
		}
		size := int64(binary.BigEndian.Uint32(hdr[0:4]))
		typ := string(hdr[4:8])
		headerLen := int64(8)

		switch size {
		case 1: // 64-bit extended size follows the header
			ext := make([]byte, 8)
			if _, err := r.ReadAt(ext, off+8); err != nil {
				return 0, 0, false
			}
			size = int64(binary.BigEndian.Uint64(ext))
			headerLen = 16
		case 0: // extends to end of the region
			size = end - off
		}

		if size < headerLen {
			return 0, 0, false // malformed
		}
		if typ == want {
			return off + headerLen, off + size, true
		}
		off += size
	}
	return 0, 0, false
}

// readMvhd parses the version and creation_time from an mvhd atom payload.
func readMvhd(r io.ReaderAt, off int64) (time.Time, bool) {
	head := make([]byte, 4) // version(1) + flags(3)
	if _, err := r.ReadAt(head, off); err != nil {
		return time.Time{}, false
	}

	var secs uint64
	if head[0] == 1 {
		b := make([]byte, 8)
		if _, err := r.ReadAt(b, off+4); err != nil {
			return time.Time{}, false
		}
		secs = binary.BigEndian.Uint64(b)
	} else {
		b := make([]byte, 4)
		if _, err := r.ReadAt(b, off+4); err != nil {
			return time.Time{}, false
		}
		secs = uint64(binary.BigEndian.Uint32(b))
	}
	if secs == 0 {
		return time.Time{}, false
	}
	return quickTimeEpoch.Add(time.Duration(secs) * time.Second), true
}
