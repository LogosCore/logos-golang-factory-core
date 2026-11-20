package factory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// Composite commands (docs: contracts/task-outsourcing.md). A command of the
// build schema may be composite (protocol.BuildCommand.Composite): its task
// runs the session's other commands as subtasks, before and after the
// command's own op, as the factory's code decides. Core calls the factory
// (factory.compose) as the task starts, at each end, when a timer runs out
// and when an operator stops the task, one call at a time, and the command's
// Composer answers each with a Decision. Run serves the calls: it checks
// each, runs the Composer within the call's deadline, MaxConcurrentDecisions
// at once (busy past them), and answers with its decision, finished (a large
// finish's output or data put as a file for the task) and checked as core
// checks it.

// The events a call comes for (ComposeCall.Event).
const (
	// Start: the task was queued, with its args.
	Start = protocol.ComposeStart
	// Ended: something the task runs ended (ComposeCall.End).
	Ended = protocol.ComposeEnded
	// Timer: the timer a decision set ran out (Decision.After).
	Timer = protocol.ComposeTimer
	// Stopped: an operator stopped the task, and what was under way has
	// ended (ComposeCall.Ends, what the stop left behind). Core takes the
	// answer's subtasks alone, to clean up, and calls no more.
	Stopped = protocol.ComposeStopped
)

// DefaultMaxConcurrentDecisions is how many calls an instance answers at
// once, unless Config says otherwise: build offers and management keep the
// rest of its 8 requests at a time.
const DefaultMaxConcurrentDecisions = 4

// Codes of the error replies a call gets for its code's sake; core ends the
// task failed with the message (CodePanic, too, for a Composer that panics).
const (
	CodeComposeFailed   = "compose_failed"   // the Composer returned an error
	CodeInvalidDecision = "invalid_decision" // its decision is one core would refuse
)

const (
	// decideMargin is how much sooner than the call's deadline a Composer's
	// context ends, so that the answer still reaches core in time.
	decideMargin = time.Second
	// maxEndBytes bounds an end's data or output fetched by link: a task's
	// output and data share 4 MiB in core.
	maxEndBytes = 4 << 20
	// maxFileName bounds the name of a file put for a task, in bytes.
	maxFileName = 255
)

// Composer is the code of a composite command.
type Composer interface {
	// Compose answers one call about one task: the subtasks to run, the
	// command's own op, what to stop, when to be called again, or the finish
	// (Decision; the zero Decision waits for the next end). It must be
	// quick, as a build's plan must: no slow work, and no waiting on anything
	// but the call's own links. ctx ends before the call's deadline. It keeps
	// nothing between calls: what a task needs from one call to the next
	// goes in its ctx (Decision.Ctx, ComposeCall.ReadCtx), so any instance
	// may answer, and a call sent again is answered again. An error fails
	// the task, with its message.
	Compose(ctx context.Context, c ComposeCall) (Decision, error)
}

// ComposerFunc adapts a function to Composer.
type ComposerFunc func(ctx context.Context, c ComposeCall) (Decision, error)

// Compose calls f.
func (f ComposerFunc) Compose(ctx context.Context, c ComposeCall) (Decision, error) { return f(ctx, c) }

// Subtask is one a decision has core queue: a command the composite uses,
// its args by param key, the factory's own key for it (which its end and a
// Cancel name it by) and a line naming it in its view.
type Subtask = protocol.Subtask

// ComposeCall is one call about a composite command's task: the call as core
// sent it (its event, the task's args, the end that brought it, the ctx the
// task's decisions left), checked, and what a Composer needs beyond it: an
// end's data and output when they came by link, the files ends list, and a
// file put for the task.
type ComposeCall struct {
	protocol.ComposeRequest
	transfers
}

// ReadCtx reads into v, as JSON, the ctx the task's decisions left
// (Decision.Ctx). With none yet, it leaves v as it is.
func (c ComposeCall) ReadCtx(v any) error {
	if len(c.Ctx) == 0 {
		return nil
	}
	if err := json.Unmarshal(c.Ctx, v); err != nil {
		return fmt.Errorf("the task's ctx: %w", err)
	}
	return nil
}

// Data is what e returned, as JSON, as core gives it to the web app: inline,
// or fetched from core when it came by link (data_url). nil when it
// returned none.
func (c ComposeCall) Data(ctx context.Context, e *protocol.ComposeEnd) (json.RawMessage, error) {
	if e == nil || e.DataURL == "" {
		if e == nil {
			return nil, nil
		}
		return e.Data, nil
	}
	b, err := c.get(ctx, e.DataURL, maxEndBytes)
	if err != nil {
		return nil, fmt.Errorf("an end's data: %w", err)
	}
	if !json.Valid(b) {
		return nil, errors.New("an end's data: core's is no JSON")
	}
	return b, nil
}

// Out is e's output, its raw bytes: inline, or fetched from core when it
// came by link (out_url). nil when it had none.
func (c ComposeCall) Out(ctx context.Context, e *protocol.ComposeEnd) ([]byte, error) {
	if e == nil || e.OutURL == "" {
		if e == nil {
			return nil, nil
		}
		return e.Out, nil
	}
	b, err := c.get(ctx, e.OutURL, maxEndBytes)
	if err != nil {
		return nil, fmt.Errorf("an end's output: %w", err)
	}
	return b, nil
}

// ReadFile fetches a file an end lists (protocol.ComposeEnd.Files), checking
// its size and SHA-256. It is for small files; Download saves large ones.
func (c ComposeCall) ReadFile(ctx context.Context, f protocol.BuildFile) ([]byte, error) {
	return c.readFile(ctx, f)
}

// Download saves a file an end lists to path, checking its size and
// SHA-256. Nothing is left at path when it fails.
func (c ComposeCall) Download(ctx context.Context, f protocol.BuildFile, path string) error {
	return c.download(ctx, f, path)
}

// PutFile puts a file for the task, at the call's UploadURL: a later
// subtask's file param may name it (its ID), as may a finish's output or
// data (OutFile, DataFile). It reads body to its end, spooled so that the
// upload has its length, and checks that core kept what it sent. name is
// the file's, without a path.
func (c ComposeCall) PutFile(ctx context.Context, name, mediaType string, body io.Reader) (protocol.BuildFile, error) {
	if err := checkFileName(name); err != nil {
		return protocol.BuildFile{}, fmt.Errorf("put %q: %w", name, err)
	}
	spooled, n, sum, err := spool(body, -1)
	if err != nil {
		return protocol.BuildFile{}, fmt.Errorf("put %s: %w", name, err)
	}
	defer unspool(spooled)
	header := http.Header{}
	header.Set(protocol.HeaderFileName, name)
	if mediaType != "" {
		header.Set("Content-Type", mediaType)
	}
	resp, err := c.put(ctx, c.UploadURL, spooled, n, header)
	if err != nil {
		return protocol.BuildFile{}, fmt.Errorf("put %s: %w", name, err)
	}
	// Its answer is read below; closing the body says nothing more.
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return protocol.BuildFile{}, fmt.Errorf("put %s: %w", name, refusal(resp))
	}
	var kept protocol.BuildFile
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&kept); err != nil {
		return protocol.BuildFile{}, fmt.Errorf("put %s: core's answer: %w", name, err)
	}
	switch {
	case kept.ID == "":
		return protocol.BuildFile{}, fmt.Errorf("put %s: core's answer names no file", name)
	case kept.Size != n || kept.SHA256 != sum:
		return protocol.BuildFile{}, fmt.Errorf("put %s: core kept %d bytes (SHA-256 %s) of %d (%s)", name, kept.Size,
			kept.SHA256, n, sum)
	}
	return protocol.BuildFile{ID: kept.ID, Name: name, Size: n, SHA256: sum, MediaType: mediaType}, nil
}

// Decision is a Composer's answer to one call: what core does next for the
// task. The zero Decision waits for the next end. Make one with RunSubtasks,
// RunOwnOp, Cancel, CancelOwnOp or Finish, and add to it with its methods;
// core applies it in this order: what it cancels, its subtasks, the own op,
// its timer. A decision is a value: each method returns a new one, and
// leaves the one it was called on as it was.
type Decision struct {
	wire protocol.Decision
	// err is what went wrong making it (a ctx that did not encode, a
	// finish's result on a decision with no finish): the call is answered
	// as an invalid decision.
	err error
}

// RunSubtasks queues subtasks, in order (Decision.RunSubtasks).
func RunSubtasks(subtasks ...Subtask) Decision { return Decision{}.RunSubtasks(subtasks...) }

// RunOwnOp sends the task's own op (Decision.RunOwnOp).
func RunOwnOp(args map[string]any) Decision { return Decision{}.RunOwnOp(args) }

// Cancel stops the subtasks with these keys (Decision.Cancel).
func Cancel(keys ...string) Decision { return Decision{}.Cancel(keys...) }

// CancelOwnOp stops the task's own op while it is out
// (Decision.CancelOwnOp).
func CancelOwnOp() Decision { return Decision{}.CancelOwnOp() }

// Finish ends the task: ok, with text as its message for the operator
// ("nothing to archive", or "" for none), or not, with text as its err (an
// empty one as "failed"). The text is cut to protocol.MaxComposeText bytes.
// Core stops what is under way, and once it has ended, the task ends so. A
// command with no op of its own gives its result with Out, Data or
// ResultOf; a finish takes nothing else but a Status.
func Finish(ok bool, text string) Decision {
	text = cutText(text, protocol.MaxComposeText)
	f := &protocol.Finish{OK: ok, Message: text}
	if !ok {
		if text == "" {
			text = "failed"
		}
		f.Message, f.Err = "", text
	}
	return Decision{wire: protocol.Decision{Finish: f}}
}

// RunSubtasks adds subtasks for core to queue, in order. Each runs a command
// the composite uses (core checks it and its args as an operator's task's: a
// subtask that fails never runs, and ends refused). A label (As) is put on
// one line and cut to protocol.MaxSubtaskLabel characters.
func (d Decision) RunSubtasks(subtasks ...Subtask) Decision {
	run := slices.Clone(d.wire.Run)
	for _, s := range subtasks {
		s.As = oneLine(s.As, protocol.MaxSubtaskLabel, false)
		run = append(run, s)
	}
	d.wire.Run = run
	return d
}

// RunOwnOp sends the task's own op, with args by param key, or the task's own
// args when args is empty. It runs once, and only for a command with an op of
// its own.
func (d Decision) RunOwnOp(args map[string]any) Decision {
	d.wire.OwnOp = &protocol.OwnOpRun{Args: args}
	return d
}

// Cancel stops every subtask not ended yet whose key is one of keys; each
// ends cancelled, and its end calls as any end does.
func (d Decision) Cancel(keys ...string) Decision {
	c := d.cancel()
	c.Keys = append(slices.Clone(c.Keys), keys...)
	d.wire.Cancel = &c
	return d
}

// CancelOwnOp stops the task's own op while it is out; it ends cancelled.
func (d Decision) CancelOwnOp() Decision {
	c := d.cancel()
	c.OwnOp = true
	d.wire.Cancel = &c
	return d
}

func (d Decision) cancel() protocol.ComposeCancel {
	if d.wire.Cancel == nil {
		return protocol.ComposeCancel{}
	}
	return *d.wire.Cancel
}

// After has core call again after wait (a Timer call), unless something ends
// first: protocol.MinComposeTimer to protocol.MaxComposeTimer. The timer stops
// at the next call, whatever brings it: each decision sets its own, or none
// (After(0)).
func (d Decision) After(wait time.Duration) Decision {
	d.wire.After = ""
	if wait != 0 {
		d.wire.After = wait.String()
	}
	return d
}

// Ctx keeps v for the task's next calls, which read it back with
// ComposeCall.ReadCtx: as JSON, or a []byte as it is; protocol.MaxComposeCtx
// bytes at most. A decision without one leaves the task's as it is; ClearCtx
// drops it.
func (d Decision) Ctx(v any) Decision {
	var b []byte
	if raw, ok := v.([]byte); ok {
		b = slices.Clone(raw)
	} else {
		enc, err := json.Marshal(v)
		if err != nil {
			d.err = errors.Join(d.err, fmt.Errorf("ctx: %w", err))
			return d
		}
		b = enc
	}
	if b == nil {
		b = []byte{}
	}
	d.wire.Ctx = &b
	return d
}

// ClearCtx drops the bytes core keeps for the task.
func (d Decision) ClearCtx() Decision {
	empty := []byte{}
	d.wire.Ctx = &empty
	return d
}

// Status sets the line shown on the task while it runs ("" clears it): put
// on one line and cut to protocol.MaxComposeText bytes. A decision without
// one leaves it as it is.
func (d Decision) Status(line string) Decision {
	s := oneLine(line, protocol.MaxComposeText, true)
	d.wire.Status = &s
	return d
}

// Out gives a finish's own output, for a command with no op of its own:
// inline up to protocol.MaxComposeInline bytes; Run puts a larger one as a
// file for the task.
func (d Decision) Out(b []byte) Decision {
	return d.withFinish("Out", func(f *protocol.Finish) { f.Out = slices.Clone(b) })
}

// Data gives a finish's own data, for a command with no op of its own: v as
// JSON, an object in the shape of the command's result. Inline up to
// protocol.MaxComposeInline bytes; Run puts a larger one as a file for the
// task.
func (d Decision) Data(v any) Decision {
	enc, err := json.Marshal(v)
	if err != nil {
		d.err = errors.Join(d.err, fmt.Errorf("data: %w", err))
		return d
	}
	return d.withFinish("Data", func(f *protocol.Finish) { f.Data = enc })
}

// ResultOf takes a subtask's result as the task's, for a command with no op
// of its own: its data, output and files. taskID is the subtask's
// (protocol.ComposeEnd.TaskID); it ended done, and its command has the
// composite's result.
func (d Decision) ResultOf(taskID string) Decision {
	return d.withFinish("ResultOf", func(f *protocol.Finish) { f.ResultOf = taskID })
}

// withFinish edits a copy of the decision's finish, or notes that it has
// none.
func (d Decision) withFinish(method string, edit func(f *protocol.Finish)) Decision {
	if d.wire.Finish == nil {
		d.err = errors.Join(d.err, fmt.Errorf("%s: a finish's result, on a decision with no finish", method))
		return d
	}
	f := *d.wire.Finish
	edit(&f)
	d.wire.Finish = &f
	return d
}

// Wire is the decision as it goes to core, before Run finishes it for its
// call (a large finish's output or data put as a file), and what went wrong
// making it.
func (d Decision) Wire() (protocol.Decision, error) { return d.wire, d.err }

// composer serves factory.compose: the factory's Composers, by command id.
type composer struct {
	code   map[string]Composer
	t      transfers
	max    int
	logger func() *slog.Logger
	now    func() time.Time
	// stopping ends with the factory: a call past it is answered busy.
	stopping context.Context

	mu      sync.Mutex
	running int
}

func newComposer(code map[string]Composer, t transfers, max int, stopping context.Context, logger func() *slog.Logger) *composer {
	return &composer{code: code, t: t, max: max, stopping: stopping, logger: logger, now: time.Now}
}

// handleCompose serves factory.compose.
func (c *composer) handleCompose(ctx context.Context, payload json.RawMessage) (any, error) {
	var req protocol.ComposeRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, coreerrors.New(protocol.CodeValidationFailed, "invalid payload: "+err.Error())
	}
	if err := req.Validate(); err != nil {
		return nil, coreerrors.New(protocol.CodeValidationFailed, clip("invalid call: "+err.Error()))
	}
	code, ok := c.code[req.Command]
	if !ok {
		return nil, coreerrors.New(protocol.CodeValidationFailed,
			fmt.Sprintf("this instance runs no composite command %q", req.Command))
	}
	by := req.Deadline.Add(-decideMargin)
	if !by.After(c.now()) {
		return nil, coreerrors.New(protocol.CodeValidationFailed, "the call's deadline has passed")
	}
	if busy := c.acquire(); busy != "" {
		c.logger().Debug("factory: a call answered busy", "task_id", req.TaskID, "command", req.Command, "reason", busy)
		return nil, coreerrors.New(protocol.CodeBusy, busy)
	}
	defer c.release()

	dctx, cancel := context.WithDeadline(ctx, by)
	defer cancel()
	call := ComposeCall{ComposeRequest: req, transfers: c.t}
	attrs := []any{"task_id", req.TaskID, "command", req.Command, "seq", req.Seq, "event", req.Event}
	d, err := callComposer(dctx, code, call)
	if err != nil {
		code := CodeComposeFailed
		var p *composerPanic
		if errors.As(err, &p) {
			code = CodePanic
		}
		c.logger().Warn("factory: a composite's code failed the task", append(attrs, "code", code, "reason", err)...)
		return nil, coreerrors.New(code, clip(err.Error()))
	}
	wire, err := d.Wire()
	if err == nil {
		wire, err = c.finish(dctx, call, wire)
	}
	if err == nil {
		err = wire.ValidateFor(req)
	}
	if err != nil {
		// Never send core a decision it would refuse: say what was wrong
		// instead.
		c.logger().Warn("factory: a composite's decision is invalid", append(attrs, "reason", err)...)
		return nil, coreerrors.New(CodeInvalidDecision, clip("invalid decision: "+err.Error()))
	}
	c.logger().Debug("factory: decided", append(attrs, "subtasks", len(wire.Run), "own_op", wire.OwnOp != nil,
		"finish", wire.Finish != nil)...)
	return wire, nil
}

// acquire takes a place for one decision, or says why there is none.
func (c *composer) acquire() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.stopping.Err() != nil:
		return "the factory is stopping"
	case c.running >= c.max:
		return fmt.Sprintf("%d of %d decisions at once", c.running, c.max)
	}
	c.running++
	return ""
}

func (c *composer) release() {
	c.mu.Lock()
	c.running--
	c.mu.Unlock()
}

// finish readies a decision for its call: a finish's own output or data past
// what goes inline is put as a file for the task, and named by its id.
func (c *composer) finish(ctx context.Context, call ComposeCall, wire protocol.Decision) (protocol.Decision, error) {
	if wire.Finish == nil {
		return wire, nil
	}
	f := *wire.Finish
	if len(f.Out) > protocol.MaxComposeInline {
		file, err := call.PutFile(ctx, "out", "application/octet-stream", bytes.NewReader(f.Out))
		if err != nil {
			return wire, fmt.Errorf("the finish's output: %w", err)
		}
		f.Out, f.OutFile = nil, file.ID
	}
	if len(f.Data) > protocol.MaxComposeInline {
		file, err := call.PutFile(ctx, "data.json", "application/json", bytes.NewReader(f.Data))
		if err != nil {
			return wire, fmt.Errorf("the finish's data: %w", err)
		}
		f.Data, f.DataFile = nil, file.ID
	}
	wire.Finish = &f
	return wire, nil
}

// composerPanic is a Composer's panic, as an error.
type composerPanic struct{ value any }

func (p *composerPanic) Error() string { return fmt.Sprint("the composite's code panicked: ", p.value) }

// callComposer runs code, turning a panic into an error.
func callComposer(ctx context.Context, code Composer, call ComposeCall) (d Decision, err error) {
	defer func() {
		if r := recover(); r != nil {
			d, err = Decision{}, &composerPanic{value: r}
		}
	}()
	return code.Compose(ctx, call)
}

// cutText cuts s to max bytes, at a character's end.
func cutText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return strings.ToValidUTF8(s[:max], "")
}

// oneLine puts s on one line (control characters as spaces) and cuts it to
// max: bytes when inBytes, else characters.
func oneLine(s string, max int, inBytes bool) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "�"))
	if inBytes {
		return cutText(s, max)
	}
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max])
}

// checkFileName checks a file's name: a name alone, no path, of maxFileName
// bytes at most.
func checkFileName(name string) error {
	switch {
	case name == "" || name == "." || name == "..":
		return errors.New("a file needs a name")
	case len(name) > maxFileName:
		return fmt.Errorf("a name of %d bytes at most", maxFileName)
	case strings.ContainsAny(name, `/\`):
		return errors.New("a name, without a path")
	case !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0:
		return errors.New("a name of text, without control characters")
	}
	return nil
}
