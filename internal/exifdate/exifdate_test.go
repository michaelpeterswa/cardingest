package exifdate

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/michaelpeterswa/cardingest/internal/card"
	"github.com/spf13/afero"
)

func le(b *bytes.Buffer, v any) { _ = binary.Write(b, binary.LittleEndian, v) }
func be(b *bytes.Buffer, v any) { _ = binary.Write(b, binary.BigEndian, v) }

// minimalTIFF builds a little-endian TIFF whose IFD0 carries a single DateTime
// (0x0132) ASCII tag — enough for goexif's DateTime() fallback.
func minimalTIFF(dt string) []byte {
	s := append([]byte(dt), 0) // NUL-terminated ASCII
	var b bytes.Buffer
	b.WriteString("II") // little-endian
	le(&b, uint16(42))  // magic
	le(&b, uint32(8))   // IFD0 at offset 8

	// IFD0 at offset 8: 1 entry.
	le(&b, uint16(1))      // entry count
	le(&b, uint16(0x0132)) // tag DateTime
	le(&b, uint16(2))      // type ASCII
	le(&b, uint32(len(s))) // count
	le(&b, uint32(26))     // value offset (string)
	le(&b, uint32(0))      // next IFD = none
	b.Write(s)             // string data at offset 26
	return b.Bytes()
}

func TestExifStillDate(t *testing.T) {
	fs := afero.NewMemMapFs()
	if err := afero.WriteFile(fs, "DSC0001.ARW", minimalTIFF("2023:05:15 10:30:00"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, src := DateOf(fs, card.FileEntry{Path: "DSC0001.ARW", ModTime: time.Unix(0, 0)})
	if src != SourceEXIF {
		t.Fatalf("source = %v, want exif", src)
	}
	if got.Format("2006-01-02") != "2023-05-15" {
		t.Fatalf("date = %s, want 2023-05-15", got.Format("2006-01-02"))
	}
}

// atom writes a QuickTime atom (size + type + payload) to b.
func atom(b *bytes.Buffer, typ string, payload []byte) {
	be(b, uint32(8+len(payload)))
	b.WriteString(typ)
	b.Write(payload)
}

// minimalMP4 builds ftyp + mdat + moov(mvhd) with the given creation time.
func minimalMP4(created time.Time) []byte {
	secs := uint32(created.Sub(quickTimeEpoch) / time.Second)

	var mvhd bytes.Buffer
	mvhd.Write([]byte{0, 0, 0, 0}) // version 0 + flags
	be(&mvhd, secs)
	mvhd.Write(make([]byte, 8)) // modification_time + timescale padding

	var moov bytes.Buffer
	atom(&moov, "mvhd", mvhd.Bytes())

	var out bytes.Buffer
	atom(&out, "ftyp", []byte("isom"))
	atom(&out, "mdat", make([]byte, 8)) // non-moov atom to walk past
	atom(&out, "moov", moov.Bytes())
	return out.Bytes()
}

func TestMP4VideoDate(t *testing.T) {
	fs := afero.NewMemMapFs()
	want := time.Date(2024, 3, 1, 8, 0, 0, 0, time.UTC)
	if err := afero.WriteFile(fs, "C0001.MP4", minimalMP4(want), 0o644); err != nil {
		t.Fatal(err)
	}

	got, src := DateOf(fs, card.FileEntry{Path: "C0001.MP4", ModTime: time.Unix(0, 0)})
	if src != SourceMP4 {
		t.Fatalf("source = %v, want mp4", src)
	}
	if !got.Equal(want) {
		t.Fatalf("date = %s, want %s", got, want)
	}
}

func TestFallbackToMtime(t *testing.T) {
	fs := afero.NewMemMapFs()
	_ = afero.WriteFile(fs, "notes.txt", []byte("hi"), 0o644)                 // unknown ext
	_ = afero.WriteFile(fs, "broken.jpg", []byte("not really a jpeg"), 0o644) // undecodable

	mt := time.Date(2022, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, name := range []string{"notes.txt", "broken.jpg"} {
		got, src := DateOf(fs, card.FileEntry{Path: name, ModTime: mt})
		if src != SourceMtime || !got.Equal(mt) {
			t.Fatalf("%s: src=%v date=%s, want mtime %s", name, src, got, mt)
		}
	}
}
