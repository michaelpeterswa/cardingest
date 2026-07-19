//go:build linux

package detect

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

const (
	defaultByPathDir   = "/dev/disk/by-path"
	defaultSysBlockDir = "/sys/class/block"
)

func newReal(cfg Config, log *slog.Logger) (Detector, error) {
	allow := make(map[string]bool, len(cfg.USBIDs))
	for _, id := range cfg.USBIDs {
		allow[strings.ToLower(strings.TrimSpace(id))] = true
	}
	return newPollDetector(&linuxEnum{
		allow:       allow,
		byPathDir:   defaultByPathDir,
		sysBlockDir: defaultSysBlockDir,
		log:         log,
	}, cfg, log), nil
}

// linuxEnum enumerates matching USB block devices by walking by-path and
// reading USB attributes from sysfs. byPathDir/sysBlockDir are fields (not
// constants) so tests can point them at a fixture tree.
type linuxEnum struct {
	allow       map[string]bool
	byPathDir   string
	sysBlockDir string
	log         *slog.Logger
}

func (e *linuxEnum) enumerate(_ context.Context) (map[card.Slot]card.DeviceInfo, error) {
	entries, err := os.ReadDir(e.byPathDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no USB block devices present yet
		}
		return nil, fmt.Errorf("read %s: %w", e.byPathDir, err)
	}

	out := make(map[card.Slot]card.DeviceInfo)
	for _, ent := range entries {
		name := ent.Name()
		// Whole-disk USB devices only: skip non-USB links and partitions.
		if !strings.Contains(name, "usb") || strings.Contains(name, "-part") {
			continue
		}

		linkPath := filepath.Join(e.byPathDir, name)
		dev, err := filepath.EvalSymlinks(linkPath)
		if err != nil {
			continue
		}

		vid, pid, serial, size, err := e.readUSBAttrs(filepath.Base(dev))
		if err != nil {
			continue
		}
		if !e.allow[vid+":"+pid] {
			continue // safety invariant #4: only allowlisted readers are touched
		}
		if size == 0 {
			// A card reader keeps its /dev/sdX node with no medium inserted;
			// size 0 means "empty slot", not an ingestable card.
			continue
		}

		slot := card.Slot(usbPortFromByPath(name))
		out[slot] = card.DeviceInfo{
			DevPath:   dev,
			ByPath:    linkPath,
			Serial:    serial,
			VendorID:  vid,
			ProductID: pid,
			SizeBytes: size,
		}
	}
	return out, nil
}

// readUSBAttrs reads idVendor/idProduct/serial and size for block device base
// (e.g. "sdb") by walking up the sysfs tree to the owning USB device.
func (e *linuxEnum) readUSBAttrs(base string) (vid, pid, serial string, size uint64, err error) {
	sysBlock := filepath.Join(e.sysBlockDir, base)

	dev, err := filepath.EvalSymlinks(sysBlock)
	if err != nil {
		return "", "", "", 0, fmt.Errorf("resolve %s: %w", sysBlock, err)
	}

	dir := dev
	for range 12 {
		dir = filepath.Dir(dir)
		if v, e := readTrimmed(filepath.Join(dir, "idVendor")); e == nil {
			vid = strings.ToLower(v)
			pid = strings.ToLower(mustReadTrimmed(filepath.Join(dir, "idProduct")))
			serial = mustReadTrimmed(filepath.Join(dir, "serial"))
			break
		}
	}
	if vid == "" {
		return "", "", "", 0, fmt.Errorf("no usb parent for %s", base)
	}

	if s, e := readTrimmed(filepath.Join(sysBlock, "size")); e == nil {
		if sectors, e2 := strconv.ParseUint(s, 10, 64); e2 == nil {
			size = sectors * 512 // sysfs size is in 512-byte sectors
		}
	}
	return vid, pid, serial, size, nil
}

func readTrimmed(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func mustReadTrimmed(path string) string {
	s, _ := readTrimmed(path)
	return s
}
