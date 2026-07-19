package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui
var uiFS embed.FS

// uiHandler serves the embedded single-page UI (and its assets) from the root.
func (s *Server) uiHandler() http.Handler {
	sub, err := fs.Sub(uiFS, "ui")
	if err != nil {
		// Should never happen: the ui directory is embedded at build time.
		return http.NotFoundHandler()
	}
	return http.FileServer(http.FS(sub))
}
