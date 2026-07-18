package mounter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

// fakeMounter simulates the mount lifecycle without touching the kernel. A
// "device" is a directory (the mock detector's DevPath). Mount copies that
// directory into a private temp location and exposes it through afero, so the
// erase phase deletes from the copy and the fixture is never harmed.
type fakeMounter struct {
	mountRoot string
	log       *slog.Logger
}

func newFake(cfg Config, log *slog.Logger) *fakeMounter {
	return &fakeMounter{mountRoot: cfg.MountRoot, log: log}
}

func (f *fakeMounter) Mount(_ context.Context, dev, _ string, o Options) (*Mount, error) {
	info, err := os.Stat(dev)
	if err != nil {
		return nil, fmt.Errorf("fake mount: stat device %s: %w", dev, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("fake mount: device %s is not a directory", dev)
	}

	// Private writable copy so erase never touches the fixture. Use the system
	// temp dir (MountRoot like /run/... may not exist off the appliance).
	tmp, err := os.MkdirTemp(tempParent(f.mountRoot), "cardingest-mock-")
	if err != nil {
		return nil, fmt.Errorf("fake mount: temp dir: %w", err)
	}
	if err := copyTree(dev, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("fake mount: copy card: %w", err)
	}

	base := afero.NewBasePathFs(afero.NewOsFs(), tmp)
	m := &Mount{
		Device:  dev,
		Target:  tmp,
		FS:      fsFor(base, o.ReadOnly),
		ro:      o.ReadOnly,
		rwBase:  base,
		cleanup: func() error { return os.RemoveAll(tmp) },
	}
	f.log.Info("fake: mounted", slog.String("device", dev), slog.String("target", tmp), slog.Bool("ro", o.ReadOnly))
	return m, nil
}

func (f *fakeMounter) Remount(_ context.Context, m *Mount, o Options) error {
	m.ro = o.ReadOnly
	m.FS = fsFor(m.rwBase, o.ReadOnly)
	f.log.Info("fake: remounted", slog.String("target", m.Target), slog.Bool("ro", o.ReadOnly))
	return nil
}

func (f *fakeMounter) Unmount(_ context.Context, m *Mount) error {
	f.log.Info("fake: unmounted", slog.String("target", m.Target))
	if m.cleanup != nil {
		return m.cleanup()
	}
	return nil
}

func (f *fakeMounter) Sync(_ context.Context, _ *Mount) error { return nil }

func (f *fakeMounter) Eject(_ context.Context, dev string) error {
	f.log.Info("fake: ejected", slog.String("device", dev))
	return nil
}

// tempParent returns a directory to create the temp copy under. It uses root
// only if it already exists and is writable; otherwise the system temp dir ("").
func tempParent(root string) string {
	if root == "" {
		return ""
	}
	if info, err := os.Stat(root); err == nil && info.IsDir() {
		return root
	}
	return ""
}

// copyTree recursively copies src into dst (which must already exist).
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		return copyFile(path, target)
	})
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
