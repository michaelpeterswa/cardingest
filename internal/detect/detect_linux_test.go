//go:build linux

package detect

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/michaelpeterswa/cardingest/internal/card"
)

// mkDevice wires a fake USB block device into a fixture sysfs + by-path tree,
// mirroring how the kernel exposes a card reader. This lets the real Linux
// enumerator be tested with no hardware and no root.
func mkDevice(t *testing.T, root, sd, byPathName, vid, pid, serial, sizeSectors string) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, v string) { must(os.WriteFile(p, []byte(v), 0o644)) }

	// The "real" sysfs device path: <root>/sys/devices/<sd>usb/host/target/block/<sd>
	usbDir := filepath.Join(root, "sys/devices", sd+"usb")
	blockDir := filepath.Join(usbDir, "host/target/block", sd)
	must(os.MkdirAll(blockDir, 0o755))
	write(filepath.Join(usbDir, "idVendor"), vid)
	write(filepath.Join(usbDir, "idProduct"), pid)
	write(filepath.Join(usbDir, "serial"), serial)
	write(filepath.Join(blockDir, "size"), sizeSectors)

	// /dev/<sd> node.
	devDir := filepath.Join(root, "dev")
	must(os.MkdirAll(devDir, 0o755))
	write(filepath.Join(devDir, sd), "")

	// /sys/class/block/<sd> -> the real block dir.
	sysClassBlock := filepath.Join(root, "sys/class/block")
	must(os.MkdirAll(sysClassBlock, 0o755))
	must(os.Symlink(blockDir, filepath.Join(sysClassBlock, sd)))

	// /dev/disk/by-path/<name> -> /dev/<sd>.
	byPathDir := filepath.Join(root, "dev/disk/by-path")
	must(os.MkdirAll(byPathDir, 0o755))
	must(os.Symlink(filepath.Join(devDir, sd), filepath.Join(byPathDir, byPathName)))
}

func TestLinuxEnumerate(t *testing.T) {
	root := t.TempDir()
	// Slot 1.1: allowlisted with media -> included.
	mkDevice(t, root, "sdb", "pci-0000:00:14.0-usb-0:1.1:1.0-scsi-0:0:0:0", "05e3", "0764", "SER123", "204800")
	// Slot 1.2: allowlisted but empty (size 0) -> excluded (no medium).
	mkDevice(t, root, "sdc", "pci-0000:00:14.0-usb-0:1.2:1.0-scsi-0:0:0:0", "174c", "2362", "SER123", "0")
	// Not allowlisted -> excluded (safety invariant #4).
	mkDevice(t, root, "sdd", "pci-0000:00:1a.0-usb-0:3:1.0-scsi-0:0:0:0", "abcd", "ef01", "OTHER", "1000")

	e := &linuxEnum{
		allow:       map[string]bool{"05e3:0764": true, "174c:2362": true},
		byPathDir:   filepath.Join(root, "dev/disk/by-path"),
		sysBlockDir: filepath.Join(root, "sys/class/block"),
		log:         testLogger(),
	}
	got, err := e.enumerate(context.Background())
	if err != nil {
		t.Fatalf("enumerate: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d devices, want 1: %+v", len(got), got)
	}
	dev, ok := got[card.Slot("1.1")]
	if !ok {
		t.Fatalf("slot 1.1 missing: %+v", got)
	}
	if dev.VendorID != "05e3" || dev.ProductID != "0764" || dev.Serial != "SER123" {
		t.Fatalf("device attrs = %+v", dev)
	}
	if dev.SizeBytes != 204800*512 {
		t.Fatalf("size = %d, want %d", dev.SizeBytes, 204800*512)
	}
}
