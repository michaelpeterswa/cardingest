package detect

import "strings"

// usbPortFromByPath extracts the stable physical port token from a
// /dev/disk/by-path name, e.g. "...-usb-0:1.1:1.0-scsi-..." -> "1.1". This
// token identifies the reader slot independent of the inserted card.
//
// It lives in a portable (untagged) file so the parsing is unit-tested on any
// OS, even though its only caller is the Linux enumerator.
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
