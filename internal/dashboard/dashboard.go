// Package dashboard is the web page the API serves at /. It is static HTML,
// CSS and JavaScript, embedded in the binary, with no build step. The page
// gets all its data from the API itself (/stats, /events, /dlq, /crons), so
// it shows exactly what jobctl and other clients see.
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static
var files embed.FS

// Handler serves the page at / and its files at /assets/{name}, and passes
// any other path, directories included, to notFound. It holds no data, so it
// needs no API token; the page asks for the token itself and sends it with
// every API call.
func Handler(notFound http.Handler) http.Handler {
	static, err := fs.Sub(files, "static")
	if err != nil {
		panic(err) // the embedded folder is fixed at build time
	}
	server := http.FileServerFS(static)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			name := strings.TrimPrefix(r.URL.Path, "/")
			if info, err := fs.Stat(static, name); err != nil || info.IsDir() {
				notFound.ServeHTTP(w, r)
				return
			}
		}
		// The files change with every build, and are small.
		w.Header().Set("Cache-Control", "no-cache")
		server.ServeHTTP(w, r)
	})
}
