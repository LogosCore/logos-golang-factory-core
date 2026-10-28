package factory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// --- fixtures ------------------------------------------------------------

const iconBody = "fake icon bytes"

func testSchema() protocol.BuildSchema {
	return protocol.BuildSchema{
		SchemaVersion: protocol.BuildSchemaV1, Version: 1,
		Channels: []protocol.BuildChannel{{
			ChannelVersions: protocol.ChannelVersions{ModuleName: "http", Versions: []int{2}},
			Fields: []protocol.FieldSpec{{Key: "method", Type: protocol.FieldEnum, Default: "POST",
				Options: []protocol.EnumOption{{Value: "GET"}, {Value: "POST"}}}},
		}},
		Fields: []protocol.FieldSpec{{Key: "greeting", Type: protocol.FieldString, Default: "hello"}},
		Targets: []protocol.BuildTarget{{
			ID: "dummy", Format: "json", Platforms: []string{"any/any"},
			Fields: []protocol.FieldSpec{{Key: "icon", Type: protocol.FieldFile, Accept: []string{".ico"}}},
		}},
		Commands: []protocol.BuildCommand{{ID: "hello", Required: true}, {ID: "shell"}},
	}
}

func sha(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// store stands in for the object store behind the presigned URLs.
type store struct {
	*httptest.Server
	mu        sync.Mutex
	uploads   map[string][]byte
	types     map[string]string
	lengths   map[string]int64
	chunked   bool
	putStatus int // when set, uploads are answered with it
}

func newStore(t *testing.T) *store {
	t.Helper()
	s := &store{uploads: map[string][]byte{}, types: map[string]string{}, lengths: map[string]int64{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.Path != "/files/app.ico" {
				http.NotFound(w, r)
				return
			}
			_, _ = io.WriteString(w, iconBody)
		case http.MethodPut:
			s.mu.Lock()
			status := s.putStatus
			s.mu.Unlock()
			if status != 0 {
				http.Error(w, "<Error><Code>AccessDenied</Code></Error>", status)
				return
			}
			body, _ := io.ReadAll(r.Body)
			s.mu.Lock()
			s.uploads[r.URL.Path], s.types[r.URL.Path], s.lengths[r.URL.Path] = body, r.Header.Get("Content-Type"), r.ContentLength
			s.chunked = s.chunked || len(r.TransferEncoding) > 0
			s.mu.Unlock()
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *store) upload(path string) ([]byte, string, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.uploads[path]
	return body, s.types[path], s.lengths[path], ok
}

func testRequest(st *store) protocol.BuildRequest {
	id := protocol.NewID()
	return protocol.BuildRequest{
		MinionID: id,
		Target:   "dummy", Platform: "any/any", SchemaVersion: 1,
		Values:   map[string]any{"greeting": "hi", "icon": "file-1"},
		Commands: []string{"hello"},
		Connections: []protocol.BuildConnection{{
			Endpoint: protocol.ChannelEndpointExport{
				Format: protocol.ChannelEndpointExportV1, Name: "edge",
				Channel: protocol.ExportChannel{ModuleName: "http", SchemaVersion: 2},
				Params:  map[string]any{"base_url": "https://edge.example.com"},
			},
			Values: map[string]any{"method": "POST"},
		}},
		Files: map[string]protocol.BuildFile{"file-1": {
			ID: "file-1", Name: "app.ico", Size: int64(len(iconBody)), SHA256: sha(iconBody),
			URL: st.URL + "/files/app.ico",
		}},
		Artifact: protocol.ArtifactUpload{UploadURL: st.URL + "/artifacts/" + id, MaxSize: 1 << 20},
		Deadline: time.Now().Add(time.Minute),
	}
}

// fakeCore records what the factory sends core. fail, when set before the
// first call, picks the reply to the n-th call of a type: nil accepts it.
type fakeCore struct {
	mu       sync.Mutex
	calls    map[string]int
	progress []protocol.BuildProgress
	results  []protocol.BuildResult
	fail     func(typ string, n int) error
}

func newFakeCore() *fakeCore { return &fakeCore{calls: map[string]int{}} }

func (c *fakeCore) Call(_ context.Context, typ string, payload, _ any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[typ]++
	if c.fail != nil {
		if err := c.fail(typ, c.calls[typ]); err != nil {
			return err
		}
	}
	switch p := payload.(type) {
	case protocol.BuildProgress:
		c.progress = append(c.progress, p)
	case *protocol.BuildResult:
		c.results = append(c.results, *p)
	}
	return nil
}

func (c *fakeCore) snapshot() (results []protocol.BuildResult, progress []protocol.BuildProgress, calls map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	calls = map[string]int{}
	for k, v := range c.calls {
		calls[k] = v
	}
	return append([]protocol.BuildResult(nil), c.results...), append([]protocol.BuildProgress(nil), c.progress...), calls
}

func (c *fakeCore) resultCount() int {
	r, _, _ := c.snapshot()
	return len(r)
}

func newTestManager(t *testing.T, b Builder, core *fakeCore, max int) *manager {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := newManager(testSchema(), b, core, http.DefaultClient, max, func() *slog.Logger { return quiet })
	m.retryMin, m.retryMax = 10*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { m.close(time.Second) })
	return m
}

func submit(t *testing.T, m *manager, req protocol.BuildRequest) error {
	t.Helper()
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := m.handleBuild(context.Background(), raw)
	if err != nil {
		return err
	}
	if r := rep.(protocol.BuildReply); r.Validate() != nil || r.MinionID != req.MinionID {
		t.Fatalf("reply %+v", r)
	}
	return nil
}

func cancelBuild(t *testing.T, m *manager, id string) {
	t.Helper()
	rep, err := m.handleCancel(context.Background(), json.RawMessage(`{"minion_id":"`+id+`"}`))
	if err != nil || !rep.(protocol.BuildCancelReply).Cancelled {
		t.Fatalf("cancel: %v %+v", err, rep)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// idle reports whether no build runs and no result is being delivered.
func idle(m *manager) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running > 0 {
		return false
	}
	for _, b := range m.builds {
		if b.delivering {
			return false
		}
	}
	return true
}

func stateOf(m *manager, id string) buildState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.builds[id].state
}

// uploadAndReturn is a builder that uploads a small artifact.
func uploadAndReturn(res Result) BuilderFunc {
	return func(ctx context.Context, job *Job) (Result, error) {
		if err := job.UploadArtifact(ctx, "minion.json", "application/json", strings.NewReader(`{"minion":true}`)); err != nil {
			return Result{}, err
		}
		return res, nil
	}
}

// waitForCancel is a builder that runs until its context ends.
func waitForCancel(started chan<- struct{}) BuilderFunc {
	return func(ctx context.Context, job *Job) (Result, error) {
		if started != nil {
			started <- struct{}{}
		}
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
}

// --- tests ---------------------------------------------------------------

func TestBuildSucceeds(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	var icon []byte
	var commands [2]bool
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		commands = [2]bool{job.HasCommand("hello"), job.HasCommand("shell")}
		f, ok := job.File("icon")
		if !ok {
			return Result{}, errors.New("no icon")
		}
		var err error
		if icon, err = job.ReadFile(ctx, f); err != nil {
			return Result{}, err
		}
		job.Step("build", "writing the minion")
		job.Logf("greeting %s", job.Request.Values["greeting"])
		if err := job.UploadArtifact(ctx, "minion.json", "application/json", strings.NewReader(`{"minion":true}`)); err != nil {
			return Result{}, err
		}
		return Result{Warnings: []string{"dummy minion"}}, nil
	}), core, 1)

	req := testRequest(st)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	results, _, _ := core.snapshot()
	r := results[0]
	want := protocol.BuildArtifact{Filename: "minion.json", Size: 15, SHA256: sha(`{"minion":true}`), MediaType: "application/json"}
	if r.Status != protocol.BuildSucceeded || r.Artifact == nil || *r.Artifact != want ||
		len(r.Warnings) != 1 || r.Log != "greeting hi\n" {
		t.Fatalf("result %+v (artifact %+v)", r, r.Artifact)
	}
	if commands != [2]bool{true, false} {
		t.Fatalf("HasCommand(hello, shell) = %v, want the request's hello only", commands)
	}
	body, ctype, length, ok := st.upload("/artifacts/" + req.MinionID)
	if !ok || string(body) != `{"minion":true}` || ctype != "application/json" || length != 15 || st.chunked {
		t.Fatalf("upload: %q %q %d chunked=%v", body, ctype, length, st.chunked)
	}
	if string(icon) != iconBody {
		t.Fatalf("icon = %q", icon)
	}
	waitFor(t, "the progress", func() bool { _, p, _ := core.snapshot(); return len(p) == 1 })
	_, progress, _ := core.snapshot()
	if p := progress[0]; p.Step != "build" || p.Message != "writing the minion" || p.MinionID != req.MinionID {
		t.Fatalf("progress %+v", p)
	}
}

// lockedBuffer is a log the manager's goroutines write while a test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// planner is a Builder that plans with steps.
type planner struct {
	steps func(req protocol.BuildRequest) []protocol.BuildStep
	build BuilderFunc
}

func (p planner) Steps(req protocol.BuildRequest) []protocol.BuildStep { return p.steps(req) }
func (p planner) Build(ctx context.Context, job *Job) (Result, error)  { return p.build(ctx, job) }

func TestPlan(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	var seen []protocol.BuildStep
	b := planner{
		steps: func(req protocol.BuildRequest) []protocol.BuildStep {
			steps := []protocol.BuildStep{{ID: "fetch", Label: "Fetch"}, {ID: "compile", Label: "Compile"}}
			if req.Values["greeting"] != "hi" {
				steps = append(steps, protocol.BuildStep{ID: "link", Label: "Link"})
			}
			return steps
		},
		build: func(ctx context.Context, job *Job) (Result, error) {
			seen = job.Steps()
			job.Step("fetch", "")
			job.Step("link", "not in this plan") // dropped
			job.Step("compile", "compiling")
			return uploadAndReturn(Result{})(ctx, job)
		},
	}
	m := newTestManager(t, b, core, 1)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	m.logger = func() *slog.Logger { return logger }

	req := testRequest(st)
	raw, _ := json.Marshal(req)
	rep, err := m.handleBuild(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	want := []protocol.BuildStep{{ID: "fetch", Label: "Fetch"}, {ID: "compile", Label: "Compile"}}
	if r := rep.(protocol.BuildReply); !slices.Equal(r.Steps, want) {
		t.Fatalf("the plan sent: %+v", r.Steps)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	if !slices.Equal(seen, want) {
		t.Fatalf("job.Steps() = %+v", seen)
	}
	waitFor(t, "the last step", func() bool {
		_, p, _ := core.snapshot()
		return len(p) > 0 && p[len(p)-1].Step == "compile"
	})
	_, progress, _ := core.snapshot()
	for _, p := range progress {
		if p.Step == "link" {
			t.Fatalf("a step outside the plan was reported: %+v", p)
		}
	}
	if !strings.Contains(logs.String(), "step=link") {
		t.Errorf("the dropped step was not logged: %s", logs.String())
	}

	// The same build again: accepted with the same plan, not rebuilt.
	rep, err = m.handleBuild(context.Background(), raw)
	if err != nil || !slices.Equal(rep.(protocol.BuildReply).Steps, want) {
		t.Fatalf("repeated: %v %+v", err, rep)
	}
}

func TestPlanFallsBack(t *testing.T) {
	for name, steps := range map[string]func(protocol.BuildRequest) []protocol.BuildStep{
		"empty": func(protocol.BuildRequest) []protocol.BuildStep { return nil },
		"invalid": func(protocol.BuildRequest) []protocol.BuildStep {
			return []protocol.BuildStep{{ID: "Fetch!", Label: "Fetch"}}
		},
		"panic": func(protocol.BuildRequest) []protocol.BuildStep { panic("no plan") },
	} {
		st, core := newStore(t), newFakeCore()
		m := newTestManager(t, planner{steps: steps, build: uploadAndReturn(Result{})}, core, 1)
		raw, _ := json.Marshal(testRequest(st))
		rep, err := m.handleBuild(context.Background(), raw)
		if err != nil || !slices.Equal(rep.(protocol.BuildReply).Steps, DefaultSteps()) {
			t.Errorf("%s: %v %+v", name, err, rep)
		}
	}

	// A build cancelled before it arrives is accepted, never built, with a
	// plan core takes.
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, BuilderFunc(uploadAndReturn(Result{})), core, 1)
	req := testRequest(st)
	cancelBuild(t, m, req.MinionID)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedBuildIsNotRebuilt(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	gate := make(chan struct{})
	var builds atomic.Int32
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		builds.Add(1)
		<-gate
		return uploadAndReturn(Result{})(ctx, job)
	}), core, 1)

	req := testRequest(st)
	for range 2 { // the second arrives while the first runs
		if err := submit(t, m, req); err != nil {
			t.Fatal(err)
		}
	}
	close(gate)
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	waitFor(t, "the delivery to end", func() bool { return idle(m) })
	// Once it finished, a repeat is answered and the result sent again.
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result again", func() bool { return core.resultCount() == 2 })
	if n := builds.Load(); n != 1 {
		t.Fatalf("built %d times", n)
	}
}

func TestBuildRefusals(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, waitForCancel(nil), core, 1)

	if _, err := m.handleBuild(context.Background(), json.RawMessage(`{"minion_id":`)); coreerrors.Code(err) != protocol.CodeValidationFailed {
		t.Errorf("malformed payload: %v", err)
	}
	for _, c := range []struct {
		name, code, mention string
		mutate              func(r *protocol.BuildRequest)
	}{
		{"schema version", protocol.CodeUnsupportedVersion, "version 1", func(r *protocol.BuildRequest) { r.SchemaVersion = 2 }},
		{"bad value", protocol.CodeValidationFailed, "values.greeting", func(r *protocol.BuildRequest) { r.Values["greeting"] = 5 }},
		{"bad connection value", protocol.CodeValidationFailed, "connections[0].values.method", func(r *protocol.BuildRequest) {
			r.Connections[0].Values["method"] = "PUT"
		}},
		{"deadline passed", protocol.CodeValidationFailed, "deadline", func(r *protocol.BuildRequest) { r.Deadline = time.Now().Add(-time.Second) }},
		{"unknown command", protocol.CodeValidationFailed, "commands[1]", func(r *protocol.BuildRequest) {
			r.Commands = append(r.Commands, "keylog")
		}},
		{"required command left out", protocol.CodeValidationFailed, "requires hello", func(r *protocol.BuildRequest) { r.Commands = nil }},
	} {
		req := testRequest(st)
		c.mutate(&req)
		err := submit(t, m, req)
		if coreerrors.Code(err) != c.code || !strings.Contains(err.Error(), c.mention) {
			t.Errorf("%s: %v", c.name, err)
		}
	}

	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatal(err)
	}
	if err := submit(t, m, testRequest(st)); coreerrors.Code(err) != protocol.CodeBusy {
		t.Fatalf("second build with room for one: %v", err)
	}
}

func TestCancel(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	started := make(chan struct{}, 1)
	var builds atomic.Int32
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		builds.Add(1)
		return waitForCancel(started)(ctx, job)
	}), core, 1)

	req := testRequest(st)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	<-started
	cancelBuild(t, m, req.MinionID)
	waitFor(t, "the build to stop", func() bool { return idle(m) })
	if n := core.resultCount(); n != 0 || stateOf(m, req.MinionID) != cancelled {
		t.Fatalf("a cancelled build reported %d results, state %v", n, stateOf(m, req.MinionID))
	}

	// A cancel that arrives first: the build is accepted, never built.
	late := testRequest(st)
	cancelBuild(t, m, late.MinionID)
	if err := submit(t, m, late); err != nil {
		t.Fatal(err)
	}
	if n := builds.Load(); n != 1 {
		t.Fatalf("built %d times", n)
	}
}

func TestCoreGivingUpStopsTheBuild(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	core.fail = func(typ string, _ int) error {
		if typ == protocol.TypeFactoryBuildProgress {
			return coreerrors.New(protocol.CodeNotFound, "no such build")
		}
		return nil
	}
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		job.Step("build", "")
		<-ctx.Done()
		return Result{}, ctx.Err()
	}), core, 1)

	req := testRequest(st)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the build to stop", func() bool { return idle(m) })
	if n := core.resultCount(); n != 0 || stateOf(m, req.MinionID) != cancelled {
		t.Fatalf("results %d, state %v", n, stateOf(m, req.MinionID))
	}
}

func TestFailedBuilds(t *testing.T) {
	for _, c := range []struct {
		name, code, message string
		builder             BuilderFunc
	}{
		{"fail", "compile_failed", "go build exited with status 1", func(context.Context, *Job) (Result, error) {
			return Result{}, Fail("compile_failed", "go build exited with status 1")
		}},
		{"plain error", CodeBuildFailed, "disk full", func(context.Context, *Job) (Result, error) {
			return Result{}, errors.New("disk full")
		}},
		{"bad code", CodeBuildFailed, "Bad Code: nope", func(context.Context, *Job) (Result, error) {
			return Result{}, Fail("Bad Code", "nope")
		}},
		{"no artifact", CodeNoArtifact, "", func(context.Context, *Job) (Result, error) { return Result{}, nil }},
		{"panic", CodePanic, "boom", func(context.Context, *Job) (Result, error) { panic("boom") }},
		{"invalid warning", CodeInvalidResult, "warning longer than", uploadAndReturn(Result{
			Warnings: []string{strings.Repeat("x", protocol.MaxBuildMessageLength+1)},
		})},
		{"too large", "artifact_too_large", "larger than", func(ctx context.Context, job *Job) (Result, error) {
			return Result{}, job.UploadArtifact(ctx, "big.bin", "", strings.NewReader(strings.Repeat("x", 1<<20+1)))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			st, core := newStore(t), newFakeCore()
			m := newTestManager(t, c.builder, core, 1)
			if err := submit(t, m, testRequest(st)); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
			results, _, _ := core.snapshot()
			r := results[0]
			if r.Status != protocol.BuildFailed || r.Error == nil || r.Error.Code != c.code ||
				!strings.Contains(r.Error.Message, c.message) || r.Artifact != nil || r.Validate() != nil {
				t.Fatalf("result %+v (error %+v)", r, r.Error)
			}
		})
	}
}

func TestResultDelivery(t *testing.T) {
	for _, c := range []struct {
		name  string
		fail  func(n int) error
		calls int  // result calls made
		ok    bool // a result was recorded
	}{
		{"lost twice", func(n int) error {
			if n <= 2 {
				return errors.New("connection lost")
			}
			return nil
		}, 3, true},
		{"refused", func(int) error { return coreerrors.New(protocol.CodeValidationFailed, "bad result") }, 1, false},
		{"core gave up", func(int) error { return coreerrors.New(protocol.CodeNotFound, "no such build") }, 1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			st, core := newStore(t), newFakeCore()
			core.fail = func(typ string, n int) error {
				if typ == protocol.TypeFactoryBuildResult {
					return c.fail(n)
				}
				return nil
			}
			m := newTestManager(t, uploadAndReturn(Result{}), core, 1)
			if err := submit(t, m, testRequest(st)); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the delivery to end", func() bool {
				_, _, calls := core.snapshot()
				return calls[protocol.TypeFactoryBuildResult] > 0 && idle(m)
			})
			results, _, calls := core.snapshot()
			if calls[protocol.TypeFactoryBuildResult] != c.calls || (len(results) == 1) != c.ok {
				t.Fatalf("calls %d, results %d", calls[protocol.TypeFactoryBuildResult], len(results))
			}
		})
	}
}

func TestTimedOutBuild(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, waitForCancel(nil), core, 1)

	// Past its deadline, core no longer waits: nothing is sent.
	req := testRequest(st)
	req.Deadline = time.Now().Add(100 * time.Millisecond)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the build to stop", func() bool { return idle(m) })
	if _, _, calls := core.snapshot(); calls[protocol.TypeFactoryBuildResult] != 0 {
		t.Fatalf("a result was sent after the deadline")
	}

	// The result says why, in case it does go out.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	r := m.result(ctx, newJob(req, testSchema().Targets[0], DefaultSteps(), http.DefaultClient), Result{}, ctx.Err())
	if r.Error == nil || r.Error.Code != CodeTimeout {
		t.Fatalf("result %+v", r.Error)
	}
}

func TestShutdownReportsRunningBuilds(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	started := make(chan struct{}, 1)
	m := newTestManager(t, waitForCancel(started), core, 1)
	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatal(err)
	}
	<-started
	m.close(2 * time.Second)
	results, _, _ := core.snapshot()
	if len(results) != 1 || results[0].Error == nil || results[0].Error.Code != CodeShutdown {
		t.Fatalf("results %+v", results)
	}
	if err := submit(t, m, testRequest(st)); coreerrors.Code(err) != protocol.CodeBusy {
		t.Fatalf("a build after close: %v", err)
	}
}
