//go:build !linux

package mounter

import "log/slog"

// newReal is unavailable off Linux: real mounting needs mount(2). Development
// uses the fake mounter instead.
func newReal(_ Config, _ *slog.Logger) (Mounter, error) {
	return nil, ErrRealModeUnsupported
}
