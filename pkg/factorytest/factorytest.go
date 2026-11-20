// Package factorytest runs a factory's builder and composites in process, for
// the factory's tests. A Store stands in for core's transfer endpoints, at
// absolute URLs a build's or a call's links can name; Run runs one build
// through the same checks, plan, Job and result as factory.Run, and returns
// what core would receive and the artifact uploaded; Compose answers one
// factory.compose call as factory.Run would.
package factorytest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/logoscore/logos-golang-factory-core/pkg/factory"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Store serves build files and takes artifacts over HTTP, as core's transfer
// endpoints do; and, for a composite's calls, serves what an end's links name
// and takes the files a factory puts for a task.
type Store struct {
	srv     *httptest.Server
	mu      sync.Mutex
	files   map[string][]byte // by path
	uploads map[string][]byte // by path
	put     map[string]Put    // by file id
	links   int
}

// Put is a file a factory put for a task: as core keeps it, and its bytes.
type Put struct {
	File    protocol.BuildFile
	TaskID  string
	Content []byte
}

// NewStore starts a Store, closed when the test ends.
func NewStore(t testing.TB) *Store {
	t.Helper()
	s := &Store{files: map[string][]byte{}, uploads: map[string][]byte{}, put: map[string]Put{}}
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
		if task, ok := taskFiles(r.URL.Path); ok {
			s.takeFile(w, r, task, body)
			return
		}
		s.mu.Lock()
		s.uploads[r.URL.Path] = body
		s.mu.Unlock()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// taskFiles reads the task of a path where files are put for it,
// /tasks/{task}/files.
func taskFiles(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "/tasks/")
	if !ok {
		return "", false
	}
	task, ok := strings.CutSuffix(rest, "/files")
	return task, ok && task != "" && !strings.Contains(task, "/")
}

// takeFile keeps a file put for task, as core does, and answers with it.
func (s *Store) takeFile(w http.ResponseWriter, r *http.Request, task string, body []byte) {
	name := r.Header.Get(protocol.HeaderFileName)
	if name == "" {
		http.Error(w, "the file needs a name ("+protocol.HeaderFileName+")", http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(body)
	f := protocol.BuildFile{ID: protocol.NewID(), Name: name, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:]),
		MediaType: r.Header.Get("Content-Type")}
	f.URL = fmt.Sprintf("%s/tasks/%s/files/%s", s.srv.URL, task, f.ID)
	s.mu.Lock()
	s.put[f.ID] = Put{File: f, TaskID: task, Content: body}
	s.files[strings.TrimPrefix(f.URL, s.srv.URL)] = body
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(f)
}

// UploadURL is where a factory puts files for task: a call's UploadURL.
func (s *Store) UploadURL(task string) string { return s.srv.URL + "/tasks/" + task + "/files" }

// Link serves content at a link of its own, as core serves an end's data or
// output that came by link (protocol.ComposeEnd.DataURL, OutURL).
func (s *Store) Link(content []byte) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.links++
	path := fmt.Sprintf("/links/%d", s.links)
	s.files[path] = content
	return s.srv.URL + path
}

// Put returns a file a factory put for a task, by its id.
func (s *Store) Put(id string) (Put, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.put[id]
	return p, ok
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
func (s *Store) Artifact(buildID string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.uploads["/artifacts/"+buildID]
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
// leaves it out: a build id, the artifact upload URL and max size, and a
// deadline a minute away. A request the factory refuses (it does not fit the
// schema) fails the test.
func Run(t testing.TB, schema protocol.BuildSchema, builder factory.Builder, store *Store, req protocol.BuildRequest) Outcome {
	t.Helper()
	if req.BuildID == "" {
		req.BuildID = protocol.NewID()
	}
	if req.Artifact.UploadURL == "" {
		req.Artifact.UploadURL = store.srv.URL + "/artifacts/" + req.BuildID
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
		out.Artifact, _ = store.Artifact(req.BuildID)
	}
	return out
}

// Compose answers call with code, as factory.Run answers a factory.compose
// (factory.ComposeOnce). It fills in what core adds to a call when call
// leaves it out: the task, session and build ids, seq 1, the start event, a
// deadline a minute away, the store's upload URL for the task, a platform
// (any/any) and an os (linux). It returns the decision core would receive,
// or the error reply core would get (a *coreerrors.Error with its code). A
// call that is not one core sends fails the test.
func Compose(t testing.TB, code factory.Composer, store *Store, call protocol.ComposeRequest) (protocol.Decision, error) {
	t.Helper()
	for _, id := range []*string{&call.TaskID, &call.SessionID, &call.BuildID} {
		if *id == "" {
			*id = protocol.NewID()
		}
	}
	if call.Seq == 0 {
		call.Seq = 1
	}
	if call.Event == "" {
		call.Event = protocol.ComposeStart
	}
	if call.Deadline.IsZero() {
		call.Deadline = time.Now().Add(time.Minute)
	}
	if call.UploadURL == "" {
		call.UploadURL = store.UploadURL(call.TaskID)
	}
	if call.Platform == "" {
		call.Platform = "any/any"
	}
	if call.OS == "" {
		call.OS = "linux"
	}
	if call.Commands == nil {
		call.Commands = []string{}
	}
	if err := call.Validate(); err != nil {
		t.Fatalf("not a call core sends: %v", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), call.Deadline)
	defer cancel()
	return factory.ComposeOnce(ctx, code, call, store.srv.Client())
}

// Build is Run for a test that only needs the result core would receive and
// the artifact uploaded (nil when the build failed).
func Build(t testing.TB, schema protocol.BuildSchema, builder factory.Builder, store *Store, req protocol.BuildRequest) (protocol.BuildResult, []byte) {
	t.Helper()
	out := Run(t, schema, builder, store, req)
	return out.Result, out.Artifact
}
