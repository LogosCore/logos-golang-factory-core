package factory

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// lines are a log's lines as "step: text".
func lines(chunks []protocol.BuildLogChunk) []string {
	var out []string
	for _, c := range chunks {
		for _, l := range c.Lines {
			out = append(out, l.Step+": "+l.Text)
		}
	}
	return out
}

// The job's log splits what is written into lines, each the step's it was
// written in: the plan's first one before any Step, a line not ended yet
// the step it was started in, a step outside the plan changing nothing.
func TestJobLogLines(t *testing.T) {
	job := testJob(newStore(t))
	at := time.Date(2026, 11, 12, 15, 0, 0, 0, time.UTC)
	job.logs.now = func() time.Time { return at }
	w := job.Log()
	job.Logf("before any step")
	_, _ = w.Write([]byte("half "))
	_, _ = w.Write([]byte("and half\r\nstarted in compile, "))
	job.Step("link", "")
	_, _ = w.Write([]byte("in link\n"))
	job.Step("sign", "not planned")
	job.Logf("still link")
	_, _ = w.Write([]byte("no end"))
	job.close()

	got, dropped, done := job.logs.take()
	want := []string{
		"compile: before any step",
		"compile: half and half",
		"compile: started in compile, ",
		"link: in link",
		"link: still link",
		"link: no end",
	}
	if !slices.Equal(lines([]protocol.BuildLogChunk{{Lines: got}}), want) || dropped != 0 || !done {
		t.Fatalf("lines %q, dropped %d, done %v", lines([]protocol.BuildLogChunk{{Lines: got}}), dropped, done)
	}
	if !got[0].At.Equal(at) {
		t.Errorf("at %v", got[0].At)
	}
	if s := job.logs.String(); !strings.HasPrefix(s, "before any step\nhalf and half\r\n") || !strings.HasSuffix(s, "no end") {
		t.Errorf("tail %q", s)
	}
}

// A line longer than a line can be goes in pieces, split at characters; one
// that does not end goes before it ends. Every chunk taken is valid.
func TestJobLogLongLines(t *testing.T) {
	l := newBuildLog("compile")
	long := strings.Repeat("é", protocol.MaxBuildLogLineBytes) // twice the limit
	_, _ = l.Write([]byte(long + "\n"))
	_, _ = l.Write([]byte(strings.Repeat("x", protocol.MaxBuildLogLineBytes+10)))
	l.close()
	var all []protocol.BuildLogLine
	for seq := int64(1); ; seq++ {
		got, _, done := l.take()
		if len(got) > 0 {
			if err := (protocol.BuildLogChunk{BuildID: protocol.NewID(), Seq: seq, Lines: got}).Validate(); err != nil {
				t.Fatal(err)
			}
		}
		all = append(all, got...)
		if done {
			break
		}
	}
	var text []string
	for _, line := range all {
		text = append(text, line.Text)
	}
	x := strings.Repeat("x", protocol.MaxBuildLogLineBytes)
	if len(text) != 4 || text[0]+text[1] != long || text[2] != x || text[3] != "xxxxxxxxxx" {
		lens := []int{}
		for _, s := range text {
			lens = append(lens, len(s))
		}
		t.Fatalf("pieces of %v bytes", lens)
	}
}

// When core falls behind, the oldest queued lines go, and the next chunk
// says how many.
func TestJobLogDropsWhenBehind(t *testing.T) {
	l := newBuildLog("compile")
	line := strings.Repeat("x", 1023) + "\n"
	n := maxQueuedLog/1023 + 100
	for range n {
		_, _ = l.Write([]byte(line))
	}
	got, dropped, _ := l.take()
	if dropped < 100 || dropped > 110 || len(got) == 0 {
		t.Fatalf("dropped %d of %d, took %d", dropped, n, len(got))
	}
	if _, again, _ := l.take(); again != 0 {
		t.Fatalf("dropped told twice: %d", again)
	}
}

func liveLogBuilder() planner {
	return planner{
		steps: func(protocol.BuildRequest) []protocol.BuildStep {
			return []protocol.BuildStep{{ID: "fetch", Label: "Fetch"}, {ID: "compile", Label: "Compile"}}
		},
		build: func(ctx context.Context, job *Job) (Result, error) {
			job.Logf("fetching")
			job.Step("compile", "")
			for i := range 1200 { // more than a chunk
				job.Logf("compiling %d", i)
			}
			return uploadAndReturn(Result{})(ctx, job)
		},
	}
}

// The log goes to core as it is written, in chunks numbered from 1, all of
// it before the result; the result still carries its end.
func TestLiveLog(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	m := newTestManager(t, liveLogBuilder(), core, 1)
	req := testRequest(st)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	chunks, order := core.logged()
	if len(chunks) < 3 {
		t.Fatalf("%d chunks", len(chunks))
	}
	for i, c := range chunks {
		if c.Seq != int64(i+1) || c.BuildID != req.BuildID || c.Validate() != nil {
			t.Fatalf("chunk %d: seq %d, %v", i, c.Seq, c.Validate())
		}
	}
	got := lines(chunks)
	if len(got) != 1201 || got[0] != "fetch: fetching" || got[1] != "compile: compiling 0" || got[1200] != "compile: compiling 1199" {
		t.Fatalf("%d lines: %q ... %q", len(got), got[:2], got[len(got)-1])
	}
	if last := slices.Index(order, protocol.TypeFactoryBuildResult); last < slices.Index(order, protocol.TypeFactoryBuildLog) ||
		slices.Contains(order[last:], protocol.TypeFactoryBuildLog) {
		t.Fatalf("log after the result: %v", order)
	}
	results, _, _ := core.snapshot()
	if !strings.HasSuffix(results[0].Log, "compiling 1199\n") || results[0].Status != protocol.BuildSucceeded {
		t.Fatalf("result %s, log ends %q", results[0].Status, results[0].Log[max(0, len(results[0].Log)-20):])
	}
}

// A chunk whose call was lost goes again with the same number.
func TestLiveLogResent(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	core.fail = func(typ string, n int) error {
		if typ == protocol.TypeFactoryBuildLog && n == 1 {
			return errors.New("lost")
		}
		return nil
	}
	m := newTestManager(t, liveLogBuilder(), core, 1)
	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	chunks, _ := core.logged()
	if chunks[0].Seq != 1 || len(lines(chunks)) != 1201 {
		t.Fatalf("first seq %d, %d lines", chunks[0].Seq, len(lines(chunks)))
	}
}

// A core that does not take the live log (one from before it refuses the
// operation as unknown) gets one chunk: the build goes on, and the result
// carries the log.
func TestLiveLogRefused(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	core.fail = func(typ string, _ int) error {
		if typ == protocol.TypeFactoryBuildLog {
			return coreerrors.New(protocol.CodeValidationFailed, `unknown operation type "factory.build.log"`)
		}
		return nil
	}
	m := newTestManager(t, liveLogBuilder(), core, 1)
	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	results, _, calls := core.snapshot()
	if calls[protocol.TypeFactoryBuildLog] != 1 {
		t.Fatalf("%d log calls after a refusal", calls[protocol.TypeFactoryBuildLog])
	}
	if results[0].Status != protocol.BuildSucceeded || !strings.HasPrefix(results[0].Log, "fetching\ncompiling 0\n") {
		t.Fatalf("result %s, log %q...", results[0].Status, results[0].Log[:30])
	}
}

// not_found to a chunk: core no longer waits for the build, which stops.
func TestLiveLogNotWaiting(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	core.fail = func(typ string, _ int) error {
		if typ == protocol.TypeFactoryBuildLog {
			return coreerrors.New(protocol.CodeNotFound, "no such build")
		}
		return nil
	}
	m := newTestManager(t, BuilderFunc(func(ctx context.Context, job *Job) (Result, error) {
		job.Logf("hello")
		<-ctx.Done()
		return Result{}, ctx.Err()
	}), core, 1)
	req := testRequest(st)
	if err := submit(t, m, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the build to stop", func() bool { return idle(m) })
	if stateOf(m, req.BuildID) != cancelled || core.resultCount() != 0 {
		t.Fatalf("state %v, %d results", stateOf(m, req.BuildID), core.resultCount())
	}
}

// A core that never answers holds the result up for logFlush at most.
func TestLiveLogFlushBounded(t *testing.T) {
	st, core := newStore(t), newFakeCore()
	core.fail = func(typ string, _ int) error {
		if typ == protocol.TypeFactoryBuildLog {
			return errors.New("lost")
		}
		return nil
	}
	m := newTestManager(t, liveLogBuilder(), core, 1)
	m.logFlush = 100 * time.Millisecond
	start := time.Now()
	if err := submit(t, m, testRequest(st)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the result", func() bool { return core.resultCount() == 1 })
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the result took %v", d)
	}
}

// BuildOnce returns the live log whole.
func TestBuildOnceLog(t *testing.T) {
	st := newStore(t)
	once, err := BuildOnce(context.Background(), testSchema(), liveLogBuilder(), testRequest(st), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := lines([]protocol.BuildLogChunk{{Lines: once.Log}}); len(got) != 1201 || got[0] != "fetch: fetching" {
		t.Fatalf("%d lines", len(got))
	}
}
