//go:build linux

package mounter

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
)

// realMounter mounts cards via mount(2). The distroless runtime has no mount
// binary, so syscalls are used directly.
type realMounter struct {
	mountRoot string
	log       *slog.Logger
}

func newReal(cfg Config, log *slog.Logger) (Mounter, error) {
	return &realMounter{mountRoot: cfg.MountRoot, log: log}, nil
}

// fsTypes lists filesystems to try, in order, when Options.FSType is empty.
var fsTypes = []string{"exfat", "vfat"}

func mountFlags(o Options, remount bool) uintptr {
	var flags uintptr
	if remount {
		flags |= unix.MS_REMOUNT
	}
	if o.ReadOnly {
		flags |= unix.MS_RDONLY
	}
	if o.NoExec {
		flags |= unix.MS_NOEXEC
	}
	if o.NoSuid {
		flags |= unix.MS_NOSUID
	}
	return flags
}

func (r *realMounter) Mount(_ context.Context, dev, target string, o Options) (*Mount, error) {
	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, fmt.Errorf("mount: create target %s: %w", target, err)
	}

	types := fsTypes
	if o.FSType != "" {
		types = []string{o.FSType}
	}

	flags := mountFlags(o, false)
	var lastErr error
	for _, fsType := range types {
		if err := unix.Mount(dev, target, fsType, flags, ""); err != nil {
			lastErr = err
			continue
		}
		base := afero.NewBasePathFs(afero.NewOsFs(), target)
		r.log.Info("mounted", slog.String("device", dev), slog.String("target", target),
			slog.String("fstype", fsType), slog.Bool("ro", o.ReadOnly))
		return &Mount{
			Device: dev,
			Target: target,
			FS:     fsFor(base, o.ReadOnly),
			ro:     o.ReadOnly,
			rwBase: base,
		}, nil
	}
	return nil, fmt.Errorf("mount %s at %s: %w", dev, target, lastErr)
}

func (r *realMounter) Remount(_ context.Context, m *Mount, o Options) error {
	flags := mountFlags(o, true)
	if err := unix.Mount("", m.Target, "", flags, ""); err != nil {
		return fmt.Errorf("remount %s: %w", m.Target, err)
	}
	m.ro = o.ReadOnly
	m.FS = fsFor(m.rwBase, o.ReadOnly)
	r.log.Info("remounted", slog.String("target", m.Target), slog.Bool("ro", o.ReadOnly))
	return nil
}

func (r *realMounter) Unmount(_ context.Context, m *Mount) error {
	if err := unix.Unmount(m.Target, 0); err != nil {
		// Retry with a lazy detach if the mount is briefly busy.
		if err2 := unix.Unmount(m.Target, unix.MNT_DETACH); err2 != nil {
			return fmt.Errorf("unmount %s: %w", m.Target, err)
		}
	}
	_ = os.Remove(m.Target)
	r.log.Info("unmounted", slog.String("target", m.Target))
	return nil
}

func (r *realMounter) Sync(_ context.Context, _ *Mount) error {
	unix.Sync()
	return nil
}

// Eject for M1 is a best-effort sync barrier; a true USB power-down is deferred.
func (r *realMounter) Eject(_ context.Context, dev string) error {
	unix.Sync()
	r.log.Info("ejected", slog.String("device", dev))
	return nil
}
