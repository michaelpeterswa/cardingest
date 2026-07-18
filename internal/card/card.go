// Package card holds the portable domain types shared across cardingest.
//
// Nothing in this package imports OS-specific machinery: it is compiled
// identically on Linux (the appliance) and darwin (development), so the
// detect, mounter, app, and pipeline packages can all speak the same
// vocabulary without pulling in syscalls.
package card

import "time"

// Slot identifies a physical reader port. It is stable for the lifetime of a
// reader (derived from the USB by-path), independent of which card is inserted.
type Slot string

const (
	SlotA Slot = "A"
	SlotB Slot = "B"
)

// DeviceInfo describes a block device that the detector has matched to the
// configured reader. Serial identifies the card (it changes when a different
// card is inserted); VendorID/ProductID identify the reader.
type DeviceInfo struct {
	DevPath   string // e.g. "/dev/sdb"
	ByPath    string // e.g. "/dev/disk/by-path/pci-...-usb-0:1.1:1.0-scsi-0:0:0:0"
	Serial    string // card/device serial, for dedupe/idempotence
	VendorID  string // USB idVendor, lowercase hex, e.g. "05dc"
	ProductID string // USB idProduct, lowercase hex
	SizeBytes uint64
}

// FileEntry is a single file discovered while scanning a mounted card.
type FileEntry struct {
	Path    string // path relative to the mount root
	Size    int64
	ModTime time.Time
}
