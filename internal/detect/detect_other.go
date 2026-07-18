//go:build !linux

package detect

import "log/slog"

// newReal is unavailable off Linux: the real detector needs sysfs and /dev.
// Development uses mock mode instead.
func newReal(_ Config, _ *slog.Logger) (Detector, error) {
	return nil, ErrRealModeUnsupported
}
