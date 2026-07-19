package detect

import "testing"

func TestUSBPortFromByPath(t *testing.T) {
	cases := map[string]string{
		"pci-0000:00:14.0-usb-0:1.1:1.0-scsi-0:0:0:0": "1.1",
		"pci-0000:00:14.0-usb-0:1.2:1.0-scsi-0:0:0:0": "1.2",
		"pci-0000:00:14.0-usb-0:2:1.0-scsi-0:0:0:0":   "2",
		"pci-0000:00:1d.0-ata-1":                      "unknown",
	}
	for in, want := range cases {
		if got := usbPortFromByPath(in); got != want {
			t.Errorf("usbPortFromByPath(%q) = %q, want %q", in, got, want)
		}
	}
}
