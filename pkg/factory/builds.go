package factory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	coreerrors "github.com/logoscore/logos-golang-module-core/pkg/errors"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// retention is how long a finished or cancelled build is remembered after
	// its deadline, so that a repeated factory.build is answered, not rebuilt.
	retention = time.Hour
	// progressTimeout bounds one progress call: progress is best effort.
	progressTimeout = 5 * time.Second
	// resultTimeout bounds one attempt to deliver a result.
	resultTimeout = 10 * time.Second
	// A result that did not reach core is sent again after minRetry,
	// doubling up to maxRetry.
	minRetry = time.Second
	maxRetry = 30 * time.Second
)

// Result codes the SDK reports itself.
const (
	CodeBuildFailed   = "build_failed"   // the builder returned an error without a code
	CodeNoArtifact    = "no_artifact"    // the builder returned without uploading
	CodeInvalidResult = "invalid_result" // the builder reported something core would refuse
	CodeTimeout       = "timeout"        // the deadline passed during the build
	CodeShutdown      = "shutdown"       // the factory stopped during the build
	CodePanic         = "panic"          // the builder panicked
)

// failureCode is what a Failure code must match to be reported as is.
var failureCode = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)

// coreCaller is the part of lifecycle.CoreClient the manager uses.
type coreCaller interface {
	Call(ctx context.Context, typ string, payload, reply any) error
}

type buildState int

const (
	running buildState = iota
	finished
	cancelled
)

// build is one accepted build, remembered until retention after its deadline.
type build struct {
	id         string
	deadline   time.Time
	steps      []protocol.BuildStep // the plan sent with the acceptance
	state      buildState
	cancel     context.CancelFunc
	result     *protocol.BuildResult // set when finished
	delivering bool
}

// manager runs builds: it serves factory.build and factory.build.cancel and
// reports progress and results to core.
type manager struct {
	schema  protocol.BuildSchema
	builder Builder
	core    coreCaller
	http    *http.Client
	logger  func() *slog.Logger
	now     func() time.Time
	// retryMin and retryMax pace result deliveries (minRetry, maxRetry).
	retryMin, retryMax time.Duration

	// ctx is the parent of every build; close cancels it first.
	ctx  context.Context
	stop context.CancelFunc
	// dctx bounds result deliveries; close cancels it after the grace.
	dctx  context.Context
	dstop context.CancelFunc
	wg    sync.WaitGroup

	mu      sync.Mutex
	builds  map[string]*build
	running int
	// max is the build limit and draining refuses every new build; both
	// are set by management (manage.go), as is what stats counts.
	max      int
	draining bool
	stats    buildStats
}

func newManager(schema protocol.BuildSchema, b Builder, core coreCaller, hc *http.Client, max int, logger func() *slog.Logger) *manager {
	m := &manager{
		schema: schema, builder: b, core: core, http: hc, max: max, logger: logger,
		now:      time.Now,
		retryMin: minRetry, retryMax: maxRetry,
		builds: map[string]*build{},
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	m.dctx, m.dstop = context.WithCancel(context.Background())
	return m
}

// handleBuild serves factory.build.
func (m *manager) handleBuild(_ context.Context, payload json.RawMessage) (any, error) {
	var req protocol.BuildRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, coreerrors.New(protocol.CodeValidationFailed, "invalid payload: "+err.Error())
	}
	accepted := func(steps []protocol.BuildStep) protocol.BuildReply {
		if steps == nil {
			steps = DefaultSteps() // a build cancelled before it arrived: never built
		}
		return protocol.BuildReply{MinionID: req.MinionID, Accepted: true, Steps: steps}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.sweepLocked()
	if b, ok := m.builds[req.MinionID]; ok {
		// The same build: accept it again, with the same plan, and never
		// build twice. A finished one reports again, in case core missed the
		// result.
		if b.state == finished && !b.delivering {
			m.deliverLocked(b)
		}
		return accepted(b.steps), nil
	}
	if problems := protocol.ValidateBuildRequest(m.schema, req); problems != nil {
		if _, ok := problems["schema_version"]; ok {
			m.logger().Warn("factory: build refused: another build schema version", "minion_id", req.MinionID,
				"requested", req.SchemaVersion, "declared", m.schema.Version)
			return nil, coreerrors.New(protocol.CodeUnsupportedVersion,
				fmt.Sprintf("this instance declares build schema version %d", m.schema.Version))
		}
		msg := describe(problems)
		m.logger().Warn("factory: build refused: invalid request", "minion_id", req.MinionID, "reason", msg)
		return nil, coreerrors.New(protocol.CodeValidationFailed, msg)
	}
	if !req.Deadline.After(m.now()) {
		return nil, coreerrors.New(protocol.CodeValidationFailed, "the deadline has passed")
	}
	busy := ""
	switch {
	case m.ctx.Err() != nil:
		busy = "the factory is stopping"
	case m.draining:
		busy = "this instance is draining"
	case m.running >= m.max:
		busy = fmt.Sprintf("%d of %d builds running", m.running, m.max)
	}
	if busy != "" {
		m.stats.refused++
		m.logger().Debug("factory: build refused: busy", "minion_id", req.MinionID, "reason", busy)
		return nil, coreerrors.New(protocol.CodeBusy, busy)
	}

	ctx, cancel := context.WithDeadline(m.ctx, req.Deadline)
	b := &build{id: req.MinionID, deadline: req.Deadline, steps: m.plan(req), state: running, cancel: cancel}
	m.builds[b.id] = b
	m.running++
	m.stats.accepted++
	m.wg.Add(1)
	go m.run(ctx, b, req)
	m.logger().Info("factory: build accepted", "minion_id", b.id, "target", req.Target, "platform", req.Platform,
		"steps", len(b.steps))
	return accepted(b.steps), nil
}

// plan is the builder's plan for a build of req, or DefaultSteps when it
// does not plan or its plan is invalid.
func (m *manager) plan(req protocol.BuildRequest) []protocol.BuildStep {
	p, ok := m.builder.(Planner)
	if !ok {
		return DefaultSteps()
	}
	steps, err := callPlanner(p, req)
	if err == nil {
		err = protocol.ValidateBuildSteps(steps)
	}
	if err != nil {
		m.logger().Error("factory: invalid build plan; the build has one step instead", "minion_id", req.MinionID, "error", err)
		return DefaultSteps()
	}
	return slices.Clone(steps)
}

// callPlanner asks p for a plan, turning a panic into an error.
func callPlanner(p Planner, req protocol.BuildRequest) (steps []protocol.BuildStep, err error) {
	defer func() {
		if r := recover(); r != nil {
			steps, err = nil, fmt.Errorf("the planner panicked: %v", r)
		}
	}()
	return p.Steps(req), nil
}

// handleCancel serves factory.build.cancel. It succeeds for any minion id; an
// unknown one is remembered, so a factory.build arriving after its cancel is
// accepted but never built.
func (m *manager) handleCancel(_ context.Context, payload json.RawMessage) (any, error) {
	var req protocol.BuildCancelRequest
	if err := json.Unmarshal(payload, &req); err != nil || req.MinionID == "" {
		return nil, coreerrors.New(protocol.CodeValidationFailed, "minion_id is required")
	}
	m.mu.Lock()
	b, ok := m.builds[req.MinionID]
	switch {
	case !ok:
		m.builds[req.MinionID] = &build{id: req.MinionID, deadline: m.now(), state: cancelled, cancel: func() {}}
	case b.state != cancelled:
		b.state = cancelled
		b.cancel()
		m.logger().Info("factory: build cancelled by core", "minion_id", b.id)
	}
	m.mu.Unlock()
	return protocol.BuildCancelReply{MinionID: req.MinionID, Cancelled: true}, nil
}

// run carries out one build and reports it.
func (m *manager) run(ctx context.Context, b *build, req protocol.BuildRequest) {
	defer m.wg.Done()
	defer b.cancel()
	started := m.now()
	target, _ := m.schema.Target(req.Target)
	job := newJob(req, target, b.steps, m.http)
	job.unknownStep = func(id string) {
		m.logger().Warn("factory: a step that is not in the build's plan was not reported", "minion_id", b.id, "step", id)
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.sendProgress(b, job.progress)
	}()

	res, err := m.callBuilder(ctx, job)
	job.close()
	result := m.result(ctx, job, res, err)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.running--
	if b.state == cancelled {
		m.stats.cancelled++
		return // core no longer waits for it
	}
	b.state, b.result = finished, &result
	m.countLocked(result)
	attrs := []any{"minion_id", b.id, "status", result.Status, "duration", m.now().Sub(started).Round(time.Millisecond).String()}
	if e := result.Error; e != nil {
		attrs = append(attrs, "code", e.Code, "reason", e.Message)
	}
	m.logger().Info("factory: build finished", attrs...)
	m.deliverLocked(b)
}

// callBuilder runs the builder, turning a panic into a failure.
func (m *manager) callBuilder(ctx context.Context, job *Job) (res Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = Result{}, Fail(CodePanic, fmt.Sprint(r))
		}
	}()
	return m.builder.Build(ctx, job)
}

// result is the factory.build.result of a build that ended with res, err.
func (m *manager) result(ctx context.Context, job *Job, res Result, err error) protocol.BuildResult {
	r := protocol.BuildResult{MinionID: job.Request.MinionID, Log: job.logs.String()}
	artifact := job.uploaded()
	switch {
	case err == nil && artifact == nil:
		err = Fail(CodeNoArtifact, "the builder returned without uploading an artifact")
	case err == nil:
		r.Status, r.Artifact, r.Capabilities, r.Warnings = protocol.BuildSucceeded, artifact, res.Capabilities, res.Warnings
	}
	if err != nil {
		code, msg := CodeBuildFailed, err.Error()
		var f *Failure
		if errors.As(err, &f) && failureCode.MatchString(f.Code) {
			code, msg = f.Code, f.Message
		}
		// A builder that stopped because its context ended says why only
		// through the context.
		if ctx.Err() != nil && code != CodePanic {
			switch {
			case m.ctx.Err() != nil:
				code, msg = CodeShutdown, "the factory stopped during the build"
			case errors.Is(ctx.Err(), context.DeadlineExceeded):
				code, msg = CodeTimeout, "the build ran out of time"
			}
		}
		r.Status, r.Error = protocol.BuildFailed, &protocol.BuildError{Code: code, Message: clip(msg)}
	}
	if verr := r.Validate(); verr != nil {
		// Never send core a result it would refuse: say what was wrong
		// instead.
		r = protocol.BuildResult{MinionID: r.MinionID, Status: protocol.BuildFailed, Log: r.Log,
			Error: &protocol.BuildError{Code: CodeInvalidResult, Message: clip(verr.Error())}}
	}
	return r
}

// sendProgress sends a build's progress to core until the job closes ch.
// Progress is best effort; when core no longer waits for the build, the build
// stops.
func (m *manager) sendProgress(b *build, ch <-chan protocol.BuildProgress) {
	for p := range ch {
		ctx, cancel := context.WithTimeout(m.dctx, progressTimeout)
		err := m.core.Call(ctx, protocol.TypeFactoryBuildProgress, p, nil)
		cancel()
		if coreerrors.Code(err) == protocol.CodeNotFound {
			m.mu.Lock()
			if b.state == running {
				b.state = cancelled
				b.cancel()
				m.logger().Info("factory: core no longer waits for the build; stopping it", "minion_id", b.id)
			}
			m.mu.Unlock()
		}
	}
}

// deliverLocked starts delivering b's result.
func (m *manager) deliverLocked(b *build) {
	b.delivering = true
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.deliver(b)
		m.mu.Lock()
		b.delivering = false
		m.mu.Unlock()
	}()
}

// deliver sends b's result until core replies, the deadline passes, or the
// factory gives up at shutdown. Only lost calls are retried: a refusal ends
// it.
func (m *manager) deliver(b *build) {
	wait := m.retryMin
	for {
		m.mu.Lock()
		state, result := b.state, b.result
		m.mu.Unlock()
		if state == cancelled {
			return
		}
		if !m.now().Before(b.deadline) {
			m.logger().Warn("factory: build result not delivered before the deadline", "minion_id", b.id)
			return
		}
		ctx, cancel := context.WithTimeout(m.dctx, resultTimeout)
		err := m.core.Call(ctx, protocol.TypeFactoryBuildResult, result, nil)
		cancel()
		var refused *coreerrors.Error
		switch {
		case err == nil:
			m.logger().Info("factory: build result recorded", "minion_id", b.id, "status", result.Status)
			return
		case errors.As(err, &refused) && refused.Code == protocol.CodeNotFound:
			m.logger().Info("factory: core no longer waits for the build", "minion_id", b.id)
			return
		case errors.As(err, &refused):
			m.logger().Warn("factory: core refused the build result", "minion_id", b.id, "code", refused.Code, "reason", refused.Message)
			return
		}
		m.logger().Warn("factory: build result not delivered; retrying", "minion_id", b.id, "err", err, "retry_in", wait.String())
		select {
		case <-m.dctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, m.retryMax)
	}
}

// close cancels every build, waits up to grace for their results to go out,
// then gives up on deliveries. A grace of zero does not wait.
func (m *manager) close(grace time.Duration) {
	m.stop()
	if grace > 0 {
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(grace):
			m.logger().Warn("factory: builds did not report within the shutdown grace", "grace", grace.String())
		}
	}
	m.dstop()
}

// sweepLocked forgets builds that ended long enough ago.
func (m *manager) sweepLocked() {
	now := m.now()
	for id, b := range m.builds {
		if b.state != running && !b.delivering && now.After(b.deadline.Add(retention)) {
			delete(m.builds, id)
		}
	}
}

// describe renders validation problems for an error message.
func describe(problems map[string]string) string {
	parts := make([]string, 0, len(problems))
	for _, k := range protocol.SortedProblemKeys(problems) {
		parts = append(parts, k+": "+problems[k])
	}
	return clip("invalid build request: " + strings.Join(parts, "; "))
}

// clip shortens a message to what a build result may carry.
func clip(s string) string {
	if len(s) <= protocol.MaxBuildMessageLength {
		return s
	}
	s = s[:protocol.MaxBuildMessageLength-len("…")]
	return strings.ToValidUTF8(s, "") + "…"
}
