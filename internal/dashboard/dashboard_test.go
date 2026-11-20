package dashboard

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// The page shows text that anyone who can submit a job chooses: job types,
// payloads, errors, cron names. It must only ever put that on the page as
// text. This guards against a later change reaching for an HTML-parsing API.
func TestScriptNeverParsesHTML(t *testing.T) {
	src, err := fs.ReadFile(files, "static/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	risky := regexp.MustCompile(`\b(innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval|new Function|setHTMLUnsafe)\b`)
	for i, line := range strings.Split(string(src), "\n") {
		if code, _, _ := strings.Cut(line, "//"); risky.MatchString(code) {
			t.Errorf("app.js:%d uses an API that parses HTML or code: %s", i+1, strings.TrimSpace(line))
		}
	}
}

func TestHandler(t *testing.T) {
	notFound := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot) // marks "passed to notFound"
	})
	h := Handler(notFound)
	for path, want := range map[string]int{
		"/":                  http.StatusOK,
		"/assets/app.js":     http.StatusOK,
		"/assets/style.css":  http.StatusOK,
		"/assets/icon.svg":   http.StatusOK,
		"/assets/":           http.StatusTeapot, // no directory listing
		"/assets":            http.StatusTeapot, // nor a redirect to one
		"/assets/nope.js":    http.StatusTeapot,
		"/assets/../go.mod":  http.StatusTeapot,
		"/static/index.html": http.StatusTeapot,
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != want {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, want)
		}
	}
}
