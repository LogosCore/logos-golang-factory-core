package factory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	composeTask = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	subtaskID   = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
)

// composeCall is a valid start of a composite with an op, its links on base.
func composeCall(base string) protocol.ComposeRequest {
	return protocol.ComposeRequest{
		TaskID: composeTask, SessionID: "1b4e28ba-2fa1-41d2-883f-0016d3cca427", BuildID: "5b0c4f6e-8a1d-4c2b-9e7f-3d2a1b0c9e8f",
		Seq: 1, Event: protocol.ComposeStart, Deadline: time.Now().Add(10 * time.Second),
		Command: "relay", Args: map[string]any{"command": "id"}, Platform: "linux/amd64", OS: "linux",
		Commands: []string{"hello", "shell", "relay"}, OwnOp: protocol.OwnOpNotRun,
		UploadURL: base + "/tasks/" + composeTask + "/files",
	}
}

func newTestComposer(code Composer, base string, max int) *composer {
	return newComposer(map[string]Composer{"relay": code}, transfers{http: http.DefaultClient, core: base}, max,
		context.Background(), func() *slog.Logger { return slog.New(slog.DiscardHandler) })
}

func compose(t *testing.T, c *composer, req protocol.ComposeRequest) (protocol.Decision, error) {
	t.Helper()
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := c.handleCompose(context.Background(), raw)
	if err != nil {
		return protocol.Decision{}, err
	}
	return rep.(protocol.Decision), nil
}

func code(err error) string { return coreerrors.Code(err) }

func decides(d Decision) ComposerFunc {
	return func(context.Context, ComposeCall) (Decision, error) { return d, nil }
}

func TestDecisionMakers(t *testing.T) {
	ls := Subtask{Command: "ls", Key: "list", As: "list\nthe folder " + strings.Repeat("é", protocol.MaxSubtaskLabel)}
	d, err := RunSubtasks(ls).RunOwnOp(map[string]any{"path": "/tmp"}).Cancel("old").CancelOwnOp().
		After(10 * time.Minute).Ctx(map[string]int{"n": 1}).Status("one\ntwo").Wire()
	switch {
	case err != nil:
		t.Fatal(err)
	case len(d.Run) != 1 || strings.ContainsRune(d.Run[0].As, '\n') || len([]rune(d.Run[0].As)) != protocol.MaxSubtaskLabel:
		t.Fatalf("subtask: %+v", d.Run)
	case d.OwnOp == nil || d.OwnOp.Args["path"] != "/tmp":
		t.Fatalf("own op: %+v", d.OwnOp)
	case d.Cancel == nil || len(d.Cancel.Keys) != 1 || !d.Cancel.OwnOp:
		t.Fatalf("cancel: %+v", d.Cancel)
	case d.After != "10m0s" || d.Ctx == nil || string(*d.Ctx) != `{"n":1}` || d.Status == nil || *d.Status != "one two":
		t.Fatalf("after %q, ctx %v, status %v", d.After, d.Ctx, d.Status)
	}

	// Each method leaves the decision it was called on as it was: three
	// subtasks leave room in their array, which two decisions made from it
	// would share otherwise.
	base := RunSubtasks(Subtask{Command: "ls"}, Subtask{Command: "du"}, Subtask{Command: "df"}).Cancel("a")
	one, _ := base.RunSubtasks(Subtask{Command: "rm"}).Cancel("b").Wire()
	two, _ := base.RunSubtasks(Subtask{Command: "cp"}).Wire()
	was, _ := base.Wire()
	if len(was.Run) != 3 || len(was.Cancel.Keys) != 1 || one.Run[3].Command != "rm" || two.Run[3].Command != "cp" ||
		len(two.Cancel.Keys) != 1 {
		t.Fatalf("shared: base %+v, one %+v, two %+v", was, one, two)
	}

	if d, _ := (Decision{}).After(time.Minute).After(0).Wire(); d.After != "" {
		t.Fatalf("After(0) = %q", d.After)
	}
	if d, _ := (Decision{}).Ctx([]byte("raw")).Wire(); string(*d.Ctx) != "raw" {
		t.Fatalf("raw ctx %q", *d.Ctx)
	}
	if d, _ := (Decision{}).ClearCtx().Wire(); d.Ctx == nil || len(*d.Ctx) != 0 {
		t.Fatalf("cleared ctx %v", d.Ctx)
	}
	if _, err := (Decision{}).Ctx(func() {}).Wire(); err == nil || !strings.Contains(err.Error(), "ctx:") {
		t.Fatalf("a ctx that does not encode: %v", err)
	}

	if d, _ := Finish(false, "").Wire(); d.Finish.OK || d.Finish.Err != "failed" || d.Finish.Message != "" {
		t.Fatalf("a silent failure: %+v", d.Finish)
	}
	long := strings.Repeat("é", protocol.MaxComposeText)
	if d, _ := Finish(true, long).Wire(); len(d.Finish.Message) > protocol.MaxComposeText || !strings.HasPrefix(long, d.Finish.Message) {
		t.Fatalf("a long message: %d bytes", len(d.Finish.Message))
	}
	if d, _ := Finish(true, "").Data(map[string]int{"n": 2}).Out([]byte("2")).ResultOf(subtaskID).Wire(); string(d.Finish.Data) != `{"n":2}` ||
		string(d.Finish.Out) != "2" || d.Finish.ResultOf != subtaskID {
		t.Fatalf("a finish's result: %+v", d.Finish)
	}
	if _, err := RunSubtasks(Subtask{Command: "ls"}).Out([]byte("x")).Wire(); err == nil || !strings.Contains(err.Error(), "no finish") {
		t.Fatalf("a result with no finish: %v", err)
	}
}

func TestComposeChecksTheCall(t *testing.T) {
	c := newTestComposer(decides(Decision{}), "http://core", 1)
	for _, tc := range []struct {
		name string
		edit func(r *protocol.ComposeRequest)
		want string
	}{
		{"not a call", nil, "invalid payload"},
		{"an invalid call", func(r *protocol.ComposeRequest) { r.Seq = 0 }, "invalid call: seq"},
		{"another command", func(r *protocol.ComposeRequest) { r.Command = "other" }, `runs no composite command "other"`},
		{"too late", func(r *protocol.ComposeRequest) { r.Deadline = time.Now().Add(decideMargin / 2) }, "deadline has passed"},
	} {
		var err error
		if tc.edit == nil {
			_, err = c.handleCompose(context.Background(), json.RawMessage(`[]`))
		} else {
			r := composeCall("http://core")
			tc.edit(&r)
			_, err = compose(t, c, r)
		}
		if code(err) != protocol.CodeValidationFailed || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want validation_failed: %q", tc.name, err, tc.want)
		}
	}
}

func TestComposeAnswers(t *testing.T) {
	var got ComposeCall
	var deadline time.Time
	c := newTestComposer(ComposerFunc(func(ctx context.Context, call ComposeCall) (Decision, error) {
		got = call
		deadline, _ = ctx.Deadline()
		return RunSubtasks(Subtask{Command: "shell", Args: map[string]any{"command": "id"}}).Status("running id"), nil
	}), "http://core", 1)
	req := composeCall("http://core")
	d, err := compose(t, c, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Run) != 1 || d.Status == nil || *d.Status != "running id" || got.TaskID != composeTask || got.Args["command"] != "id" {
		t.Fatalf("decision %+v, call %+v", d, got.ComposeRequest)
	}
	// The code's context ends before the call's deadline, so its answer
	// still reaches core.
	if want := req.Deadline.Add(-decideMargin); deadline.Sub(want).Abs() > 10*time.Millisecond {
		t.Fatalf("context deadline %v, want %v", deadline, want)
	}
}

func TestComposeFailures(t *testing.T) {
	ended := composeCall("http://core")
	ended.Event, ended.OwnOp = protocol.ComposeEnded, protocol.OwnOpEnded
	ended.End = &protocol.ComposeEnd{TaskID: composeTask, OwnOp: true, Command: "relay", State: protocol.EndDone, OK: true}
	for _, tc := range []struct {
		name string
		code ComposerFunc
		call protocol.ComposeRequest
		want string // the code
		msg  string
	}{
		{"an error", func(context.Context, ComposeCall) (Decision, error) { return Decision{}, errors.New("no idea") },
			composeCall("http://core"), CodeComposeFailed, "no idea"},
		{"a panic", func(context.Context, ComposeCall) (Decision, error) { panic("boom") },
			composeCall("http://core"), CodePanic, "panicked: boom"},
		{"its own op again", decides(RunOwnOp(nil)), ended, CodeInvalidDecision, "sent already"},
		{"a finish with more", decides(Finish(true, "").Ctx(1)), composeCall("http://core"), CodeInvalidDecision, "stands alone"},
		{"a ctx that does not encode", decides(RunSubtasks(Subtask{Command: "shell"}).Ctx(make(chan int))),
			composeCall("http://core"), CodeInvalidDecision, "ctx:"},
		{"a timer too short", decides(Decision{}.After(time.Millisecond)), composeCall("http://core"), CodeInvalidDecision, "after"},
		{"a result beside its own op", decides(Finish(true, "").Out([]byte("x"))), composeCall("http://core"), CodeInvalidDecision,
			"result is its own op's"},
	} {
		_, err := compose(t, newTestComposer(tc.code, "http://core", 1), tc.call)
		if code(err) != tc.want || !strings.Contains(err.Error(), tc.msg) {
			t.Errorf("%s: %v, want %s: %q", tc.name, err, tc.want, tc.msg)
		}
	}
}

func TestComposeBusy(t *testing.T) {
	// A call's code waits to be let go, or for its context: a broken limit
	// fails the test within the calls' deadline, never hangs it.
	entered, release := make(chan struct{}), make(chan struct{})
	c := newTestComposer(ComposerFunc(func(ctx context.Context, _ ComposeCall) (Decision, error) {
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return Decision{}, ctx.Err()
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return Decision{}, nil
	}), "http://core", 2)
	call := func() protocol.ComposeRequest {
		r := composeCall("http://core")
		r.Deadline = time.Now().Add(3 * time.Second)
		return r
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := compose(t, c, call()); err != nil {
				t.Error(err)
			}
		})
		<-entered
	}
	if _, err := compose(t, c, call()); code(err) != protocol.CodeBusy || !strings.Contains(err.Error(), "2 of 2") {
		t.Fatalf("a third call: %v, want busy", err)
	}
	close(release)
	wg.Wait()
	go func() { <-entered }()
	if _, err := compose(t, c, call()); err != nil {
		t.Fatalf("after the two: %v", err)
	}

	stopping, stop := context.WithCancel(context.Background())
	stop()
	c.stopping = stopping
	if _, err := compose(t, c, composeCall("http://core")); code(err) != protocol.CodeBusy || !strings.Contains(err.Error(), "stopping") {
		t.Fatalf("a stopping factory: %v, want busy", err)
	}
}

// coreFiles stands in for core's ends' links and the files put for a task.
type coreFiles struct {
	*httptest.Server
	mu   sync.Mutex
	put  map[string][]byte // by name
	keep func(name string, body []byte) protocol.BuildFile
}

func newCoreFiles(t *testing.T) *coreFiles {
	t.Helper()
	cf := &coreFiles{put: map[string][]byte{}}
	cf.keep = func(_ string, body []byte) protocol.BuildFile {
		return protocol.BuildFile{ID: "d4f9a1c2-5b3e-4c7d-8e9f-0a1b2c3d4e5f", Size: int64(len(body)), SHA256: sha(string(body))}
	}
	cf.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/data":
			_, _ = io.WriteString(w, `{"entries": []}`)
		case r.Method == http.MethodGet && r.URL.Path == "/out":
			_, _ = io.WriteString(w, "uid=0(root)")
		case r.Method == http.MethodGet && r.URL.Path == "/files/a.txt":
			_, _ = io.WriteString(w, iconBody)
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/files"):
			body, _ := io.ReadAll(r.Body)
			name := r.Header.Get(protocol.HeaderFileName)
			cf.mu.Lock()
			cf.put[name] = body
			keep := cf.keep
			cf.mu.Unlock()
			if name == "refused" {
				http.Error(w, "too large", http.StatusRequestEntityTooLarge)
				return
			}
			_ = json.NewEncoder(w).Encode(keep(name, body))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(cf.Close)
	return cf
}

func TestComposeCallLinks(t *testing.T) {
	cf := newCoreFiles(t)
	call := ComposeCall{ComposeRequest: composeCall(""), transfers: transfers{http: http.DefaultClient, core: cf.URL}}
	ctx := context.Background()
	e := &protocol.ComposeEnd{DataURL: "/data", OutURL: "/out"}
	data, err := call.Data(ctx, e)
	if err != nil || string(data) != `{"entries": []}` {
		t.Fatalf("data by link: %q, %v", data, err)
	}
	out, err := call.Out(ctx, e)
	if err != nil || string(out) != "uid=0(root)" {
		t.Fatalf("output by link: %q, %v", out, err)
	}
	if data, _ := call.Data(ctx, &protocol.ComposeEnd{Data: json.RawMessage(`{"n":1}`)}); string(data) != `{"n":1}` {
		t.Fatalf("data inline: %q", data)
	}
	if out, _ := call.Out(ctx, nil); out != nil {
		t.Fatalf("no end: %q", out)
	}

	f := protocol.BuildFile{Name: "a.txt", Size: int64(len(iconBody)), SHA256: sha(iconBody), URL: "/files/a.txt"}
	if b, err := call.ReadFile(ctx, f); err != nil || string(b) != iconBody {
		t.Fatalf("a file: %q, %v", b, err)
	}
	f.SHA256 = sha("other")
	if _, err := call.ReadFile(ctx, f); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("a file that does not check out: %v", err)
	}

	kept, err := call.PutFile(ctx, "settings.ini", "text/plain", strings.NewReader("debug=1\n"))
	if err != nil || kept.ID == "" || kept.Name != "settings.ini" || kept.Size != 8 || string(cf.put["settings.ini"]) != "debug=1\n" {
		t.Fatalf("put: %+v, %v", kept, err)
	}
	for _, tc := range []struct{ name, want string }{
		{"../x", "without a path"},
		{"", "needs a name"},
		{"refused", "413"},
	} {
		if _, err := call.PutFile(ctx, tc.name, "", strings.NewReader("x")); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("put %q: %v, want %q", tc.name, err, tc.want)
		}
	}
	cf.mu.Lock()
	cf.keep = func(string, []byte) protocol.BuildFile { return protocol.BuildFile{ID: "x", Size: 1, SHA256: sha("y")} }
	cf.mu.Unlock()
	if _, err := call.PutFile(ctx, "b.bin", "", strings.NewReader("xy")); err == nil || !strings.Contains(err.Error(), "core kept 1 bytes") {
		t.Fatalf("a file core kept otherwise: %v", err)
	}
}

// A finish's own output or data past what goes inline is put as a file for
// the task, and the decision names it.
func TestComposePutsLargeResults(t *testing.T) {
	cf := newCoreFiles(t)
	big := strings.Repeat("x", protocol.MaxComposeInline+1)
	c := newTestComposer(decides(Finish(true, "").Out([]byte(big)).Data(map[string]string{"s": big})), cf.URL, 1)
	req := composeCall("")
	req.OwnOp = "" // a composite with no op of its own gives its own result
	d, err := compose(t, c, req)
	if err != nil {
		t.Fatal(err)
	}
	f := d.Finish
	if f.Out != nil || f.Data != nil || f.OutFile == "" || f.DataFile == "" || string(cf.put["out"]) != big ||
		!strings.Contains(string(cf.put["data.json"]), big) {
		t.Fatalf("finish %+v", f)
	}
}
