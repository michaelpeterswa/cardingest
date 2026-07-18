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

func newReal(cfg Config, log *slog.Logger) (Detector, error) {
	allow := make(map[string]bool, len(cfg.USBIDs))
	for _, id := range cfg.USBIDs {
		allow[strings.ToLower(strings.TrimSpace(id))] = true
	}
	return newPollDetector(&linuxEnum{allow: allow, log: log}, cfg, log), nil
}

// linuxEnum enumerates matching USB block devices by walking
// /dev/disk/by-path and reading USB attributes from sysfs.
type linuxEnum struct {
	allow map[string]bool
	log   *slog.Logger
}

const byPathDir = "/dev/disk/by-path"

func (e *linuxEnum) enumerate(_ context.Context) (map[card.Slot]card.DeviceInfo, error) {
	entries, err := os.ReadDir(byPathDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no USB block devices present yet
		}
		return nil, fmt.Errorf("read %s: %w", byPathDir, err)
	}

	out := make(map[card.Slot]card.DeviceInfo)
	for _, ent := range entries {
		name := ent.Name()
		// Whole-disk USB devices only: skip non-USB links and partitions.
		if !strings.Contains(name, "usb") || strings.Contains(name, "-part") {
			continue
		}

		linkPath := filepath.Join(byPathDir, name)
		dev, err := filepath.EvalSymlinks(linkPath)
		if err != nil {
			continue
		}

		vid, pid, serial, size, err := readUSBAttrs(filepath.Base(dev))
		if err != nil {
			continue
		}
		if !e.allow[vid+":"+pid] {
			continue // safety invariant #4: only allowlisted readers are touched
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

// usbPortFromByPath extracts the stable physical port token from a by-path
// name, e.g. "...-usb-0:1.1:1.0-scsi-..." -> "1.1". This token identifies the
// reader slot independent of the inserted card.
func usbPortFromByPath(name string) string {
	_, rest, ok := strings.Cut(name, "usb-")
	if !ok {
		return "unknown"
	}
	if j := strings.IndexByte(rest, '-'); j >= 0 {
		rest = rest[:j]
	}
	parts := strings.Split(rest, ":")
	if len(parts) >= 2 {
		return parts[1]
	}
	return rest
}

// readUSBAttrs reads idVendor/idProduct/serial and size for block device base
// (e.g. "sdb") by walking up the sysfs tree to the owning USB device.
func readUSBAttrs(base string) (vid, pid, serial string, size uint64, err error) {
	sysBlock := filepath.Join("/sys/class/block", base)

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
