// Package factorytest runs a factory's builder in process, for the factory's
// tests. A Store stands in for the object store behind a build's presigned
// URLs; Run runs one build through the same checks, plan, Job and result as
// factory.Run, and returns what core would receive and the artifact uploaded.
package factorytest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-factory-core/pkg/factory"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Store serves build files and takes artifacts over HTTP, like the object
// store behind presigned URLs.
type Store struct {
	srv     *httptest.Server
	mu      sync.Mutex
	files   map[string][]byte // by path
	uploads map[string][]byte // by path
}

// NewStore starts a Store, closed when the test ends.
func NewStore(t testing.TB) *Store {
	t.Helper()
	s := &Store{files: map[string][]byte{}, uploads: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *Store) serve(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		body, ok := s.files[r.URL.Path]
		s.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(body)
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.uploads[r.URL.Path] = body
		s.mu.Unlock()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// File stores content and returns it as a build request carries it, for
// Request.Files and a file field's value (its ID).
func (s *Store) File(id, name, mediaType string, content []byte) protocol.BuildFile {
	path := "/files/" + id
	s.mu.Lock()
	s.files[path] = content
	s.mu.Unlock()
	sum := sha256.Sum256(content)
	return protocol.BuildFile{ID: id, Name: name, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:]),
		MediaType: mediaType, URL: s.srv.URL + path}
}

// Artifact returns what a build uploaded.
func (s *Store) Artifact(minionID string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.uploads["/artifacts/"+minionID]
	return body, ok
}

// MaxArtifactSize is the max_size Build gives a request that has none.
const MaxArtifactSize = 64 << 20

// Outcome is what a build Run ran left: what core would have received (the
// plan, the progress reports that reached it, the result) and the artifact
// uploaded (nil when the build failed).
type Outcome struct {
	factory.Once
	Artifact []byte
}

// Run runs builder on req. It fills in what core adds to a request when req
// leaves it out: a minion id, the artifact upload URL and max size, and a
// deadline a minute away. A request the factory refuses (it does not fit the
// schema) fails the test.
func Run(t testing.TB, schema protocol.BuildSchema, builder factory.Builder, store *Store, req protocol.BuildRequest) Outcome {
	t.Helper()
	if req.MinionID == "" {
		req.MinionID = protocol.NewID()
	}
	if req.Artifact.UploadURL == "" {
		req.Artifact.UploadURL = store.srv.URL + "/artifacts/" + req.MinionID
	}
	if req.Artifact.MaxSize == 0 {
		req.Artifact.MaxSize = MaxArtifactSize
	}
	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(time.Minute)
	}
	ctx, cancel := context.WithDeadline(context.Background(), req.Deadline)
	defer cancel()
	once, err := factory.BuildOnce(ctx, schema, builder, req, store.srv.Client())
	if err != nil {
		t.Fatalf("factory refused the build: %v", err)
	}
	out := Outcome{Once: once}
	if once.Result.Status == protocol.BuildSucceeded {
		out.Artifact, _ = store.Artifact(req.MinionID)
	}
	return out
}

// Build is Run for a test that only needs the result core would receive and
// the artifact uploaded (nil when the build failed).
func Build(t testing.TB, schema protocol.BuildSchema, builder factory.Builder, store *Store, req protocol.BuildRequest) (protocol.BuildResult, []byte) {
	t.Helper()
	out := Run(t, schema, builder, store, req)
	return out.Result, out.Artifact
}
