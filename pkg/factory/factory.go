// Package factory runs a Logos factory module: module-core's control-plane
// session as module type "factory", with the factory's build schema declared
// at registration and its builds served on the rpc_queue.
//
// A factory implements Builder, and Planner to name the steps of each build.
// For each factory.build core sends, Run checks the request against the
// schema, accepts it with the build's plan (or replies busy), and calls Build
// with a Job: the request, the plan and the step it is on, a build log, the
// request's files and the artifact upload. When Build returns, Run sends
// factory.build.result to core until core acknowledges it or the build's
// deadline passes. See the docs repo: contracts/minion-build.md.
//
// Every factory is managed for its builds: Run adds a builds section to the
// factory's management (manage.go), with the build limit, drain and resume,
// and build counters, next to whatever the factory declares itself.
package factory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"time"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/mgmt"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// ModuleType is the module type every factory registers with.
const ModuleType = protocol.ServiceFactory

// RPCQueue is a factory instance's rpc_queue: logos.factory.rpc.<instance>.
func RPCQueue(instance string) string { return module.RPCQueue(ModuleType, instance) }

// Defaults.
const (
	DefaultMaxConcurrentBuilds = 1
	// DefaultShutdownGrace is how long Run waits at shutdown for the builds
	// it cancelled to report before it deregisters.
	DefaultShutdownGrace = 10 * time.Second
)

// Builder builds minions.
type Builder interface {
	// Build builds one minion from job and uploads its artifact with
	// job.UploadArtifact, reporting each step of the plan it starts with
	// job.Step. It must return when ctx is done: the build was cancelled, ran
	// out of time, or the factory is stopping. An error fails the build; Fail
	// gives it a code of the factory's own.
	Build(ctx context.Context, job *Job) (Result, error)
}

// Planner is a Builder that names the steps of a build before it starts.
// A builder without it has one step, DefaultSteps.
type Planner interface {
	// Steps is the plan for a build of req, a request that fits the schema:
	// the steps Build goes through, in order, for these parameters (a step
	// that will not run for them is left out). Core gets it with the
	// acceptance and shows it as the build's pipeline. It must be quick and
	// valid (protocol.ValidateBuildSteps); an invalid plan is logged and
	// DefaultSteps used instead.
	Steps(req protocol.BuildRequest) []protocol.BuildStep
}

// DefaultStep is the one step of a builder that does not plan.
var DefaultStep = protocol.BuildStep{ID: "build", Label: "Build"}

// DefaultSteps is the plan of a builder that does not plan.
func DefaultSteps() []protocol.BuildStep { return []protocol.BuildStep{DefaultStep} }

// BuilderFunc adapts a function to Builder.
type BuilderFunc func(ctx context.Context, job *Job) (Result, error)

// Build calls f.
func (f BuilderFunc) Build(ctx context.Context, job *Job) (Result, error) { return f(ctx, job) }

// Result is what a successful build reports next to its artifact. What the
// minion can do is no part of it: the commands of the request
// (Job.HasCommand), which the builder builds in, all of them or fails.
type Result struct {
	// Warnings are shown with the build, e.g. "debug build: strings are not
	// obfuscated".
	Warnings []string
}

// Failure is a build error with a code of the factory's own, such as
// "compile_failed", reported in the result. Any other error is reported as
// "build_failed".
type Failure struct {
	Code    string
	Message string
}

func (f *Failure) Error() string { return f.Code + ": " + f.Message }

// Fail returns a Failure. code must match ^[a-z0-9][a-z0-9_.-]{0,62}$, or
// the build is reported as "build_failed".
func Fail(code, message string) error { return &Failure{Code: code, Message: message} }

// Config describes a factory instance.
type Config struct {
	// Config is the module session. Identity.ModuleType defaults to
	// ModuleType; any other is refused. Handlers may not take the build
	// operations, which Run serves itself. Core defaults to a new client.
	//
	// Management is the factory's own: Run adds the builds section to it,
	// so its manifest cannot use the section's keys (manage.go), and one
	// server serves one Run. Without it, Run makes the management of just
	// the builds section, which Logs can be served with.
	module.Config
	// Build is the build schema declared at registration. Required.
	Build *protocol.BuildSchema
	// Builder builds the minions. Required.
	Builder Builder
	// MaxConcurrentBuilds is how many builds run at once; a factory.build
	// beyond it is refused with busy. Defaults to DefaultMaxConcurrentBuilds.
	// It is where the max_concurrent_builds property starts.
	MaxConcurrentBuilds int
	// HTTPClient downloads files and uploads artifacts. It needs no timeout
	// of its own: every transfer is bounded by its build's deadline.
	// Defaults to a plain http.Client.
	HTTPClient *http.Client
	// ShutdownGrace defaults to DefaultShutdownGrace.
	ShutdownGrace time.Duration
}

// Run keeps the factory registered and serving builds until ctx is
// cancelled. Then it cancels the running builds, waits up to ShutdownGrace
// for them to report, deregisters and returns nil. It returns an error only
// for an invalid Config.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Identity.ModuleType == "" {
		cfg.Identity.ModuleType = ModuleType
	}
	if cfg.Identity.ModuleType != ModuleType {
		return fmt.Errorf("factory: module type must be %q, got %q", ModuleType, cfg.Identity.ModuleType)
	}
	if cfg.Build == nil {
		return errors.New("factory: Build schema is required")
	}
	if err := protocol.ValidateBuildSchema(*cfg.Build); err != nil {
		return fmt.Errorf("factory: invalid build schema: %w", err)
	}
	if cfg.Builder == nil {
		return errors.New("factory: Builder is required")
	}
	switch {
	case cfg.MaxConcurrentBuilds == 0:
		cfg.MaxConcurrentBuilds = DefaultMaxConcurrentBuilds
	case cfg.MaxConcurrentBuilds < 0:
		return errors.New("factory: MaxConcurrentBuilds must not be negative")
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = DefaultShutdownGrace
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	if cfg.Core == nil {
		cfg.Core = lifecycle.NewCoreClient()
	}
	for _, typ := range []string{protocol.TypeFactoryBuild, protocol.TypeFactoryBuildCancel} {
		if _, taken := cfg.Handlers[typ]; taken {
			return fmt.Errorf("factory: Handlers must not take %q", typ)
		}
	}
	logger := func() *slog.Logger {
		if cfg.Logger != nil {
			return cfg.Logger
		}
		return slog.Default()
	}

	m := newManager(*cfg.Build, cfg.Builder, cfg.Core, cfg.HTTPClient, cfg.MaxConcurrentBuilds, logger)
	mc := cfg.Config
	mc.Handlers = maps.Clone(cfg.Handlers)
	if mc.Handlers == nil {
		mc.Handlers = map[string]mgmt.HandlerFunc{}
	}
	mc.Handlers[protocol.TypeFactoryBuild] = m.handleBuild
	mc.Handlers[protocol.TypeFactoryBuildCancel] = m.handleCancel
	mc.Options = append([]lifecycle.Option{lifecycle.WithRegisterField(protocol.RegisterFieldBuild, *cfg.Build)}, cfg.Options...)
	if err := manage(&mc, m); err != nil {
		m.close(0)
		return fmt.Errorf("factory: %w", err)
	}

	// The session outlives ctx: builds cancelled at shutdown still report to
	// core, and only then does the factory deregister.
	sessionCtx, stopSession := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSession()
	done := make(chan error, 1)
	go func() { done <- module.Run(sessionCtx, mc) }()
	select {
	case err := <-done: // an invalid module config; nothing was served
		m.close(0)
		return err
	case <-ctx.Done():
	}
	m.close(cfg.ShutdownGrace)
	stopSession()
	return <-done
}
