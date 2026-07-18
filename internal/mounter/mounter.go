// Package mounter owns the card mount lifecycle: mount read-only for the whole
// ingest/verify phase, remount read-write only after verification, targeted
// deletes, sync, then unmount/eject.
//
// Mounter is a hardware-abstraction boundary. The real implementation
// (mounter_linux.go) issues mount(2) syscalls; the fake (fake.go) exposes a
// copied directory through an afero filesystem so the whole pipeline runs and
// is tested off Linux. Portable code only ever touches Mount.FS, never os.* or
// syscalls, which is what makes the two interchangeable.
package mounter

import (
	"context"
	"errors"
	"log/slog"

	"github.com/spf13/afero"
)

// Options controls how a card is mounted. NoExec/NoSuid are always set for card
// filesystems; ReadOnly is true for the ingest/verify phase and cleared only by
// Remount before the erase phase.
type Options struct {
	ReadOnly bool
	NoExec   bool
	NoSuid   bool
	FSType   string // "" lets the kernel auto-detect (exfat/vfat)
}

// Mount is a live mount. Portable callers read and (after Remount) write files
// exclusively through FS.
type Mount struct {
	Device string
	Target string
	FS     afero.Fs

	ro      bool
	rwBase  afero.Fs     // writable base fs; FS wraps it read-only while ro
	cleanup func() error // impl-specific teardown run by Unmount
}

// ReadOnly reports whether the mount is currently read-only.
func (m *Mount) ReadOnly() bool { return m.ro }

// Mounter manages the mount lifecycle for one card at a time.
type Mounter interface {
	Mount(ctx context.Context, dev, target string, o Options) (*Mount, error)
	Remount(ctx context.Context, m *Mount, o Options) error // ro→rw, only after verify
	Unmount(ctx context.Context, m *Mount) error
	Sync(ctx context.Context, m *Mount) error
	Eject(ctx context.Context, dev string) error
}

// Config selects and configures a Mounter.
type Config struct {
	Mock      bool
	MountRoot string // where real mounts are rooted; a temp hint for the fake
}

// ErrRealModeUnsupported is returned when the real mounter is requested on a
// non-Linux platform.
var ErrRealModeUnsupported = errors.New("mounter: real mode is only supported on linux")

// New builds a Mounter for the configuration.
func New(cfg Config, log *slog.Logger) (Mounter, error) {
	if cfg.Mock {
		return newFake(cfg, log), nil
	}
	return newReal(cfg, log)
}

// fsFor wraps base read-only when ro is set.
func fsFor(base afero.Fs, ro bool) afero.Fs {
	if ro {
		return afero.NewReadOnlyFs(base)
	}
	return base
}
