# logos-golang-factory-core

Go SDK for Logos factory modules: the modules that build minions. It builds on
[`logos-golang-module-core`](https://github.com/logoscore/logos-golang-module-core)
(registration, liveness, management, logs) and adds what a factory does: it
declares a build schema at registration and serves builds.

The contract is the Minion Build page of the docs repo
(`contracts/minion-build.md`).

## Usage

A factory declares its build schema and implements `Builder`:

```go
schema := protocol.BuildSchema{ /* targets, fields, channels */ }
err := factory.Run(ctx, factory.Config{
	Config: module.Config{
		AMQPURL:  amqpURL,
		Identity: lifecycle.Identity{ModuleName: "bob", Instance: "bob-1", Version: version},
	},
	Build: &schema,
	Builder: factory.BuilderFunc(func(ctx context.Context, job *factory.Job) (factory.Result, error) {
		job.Progress("render", "writing the minion", 50)
		minion := render(job.Request) // values, connections, files
		if err := job.UploadArtifact(ctx, "minion.json", "application/json", minion); err != nil {
			return factory.Result{}, err
		}
		return factory.Result{Capabilities: []string{"hello"}}, nil
	}),
})
```

## What Run does for a build

- Checks each `factory.build` against the schema (`protocol.ValidateBuildRequest`)
  and replies at once: accepted, `validation_failed`, `unsupported_version`
  (another schema version), or `busy` (the build limit already running, the
  instance drained, or the factory stopping).
- Answers a repeated build id without building twice, and sends a finished
  build's result again.
- Runs `Builder.Build` with a context that ends at the build's deadline, when
  core cancels it (`factory.build.cancel`, or `not_found` on progress), or when
  the factory stops.
- Sends `factory.build.result` to core until core replies or the deadline
  passes. A builder error becomes a failed result: `Fail(code, message)` for a
  code of the factory's own, otherwise `build_failed`; the SDK's own codes are
  `no_artifact`, `invalid_result`, `timeout`, `shutdown` and `panic`.
- At shutdown it cancels running builds, gives them `ShutdownGrace` to report,
  then deregisters.

The `Job` a builder gets carries the request and target, `Progress` (best
effort, never blocks), a build log (`Log`, `Logf`; its last 64 KiB go with the
result), `File`, `ReadFile` and `Download` (checked by size and SHA-256), and
`UploadArtifact` (computes the size and SHA-256, enforces `max_size`).

## Management

Every factory is managed for its builds. Run adds a **builds section** to the
factory's management (`mgmt.Server.Extend`), next to whatever the factory
declares itself in `module.Config.Management`, or as its whole management when
it declares none:

| | |
|---|---|
| Property | `max_concurrent_builds` (live; starts at `MaxConcurrentBuilds`) |
| Actions | `drain` (take no new builds; running ones finish), `resume`; each available only when it changes something (`available_when` on `draining`) |
| Status | `builds_running`, `builds_accepted_total`, `builds_succeeded_total`, `builds_failed_total`, `builds_cancelled_total`, `builds_refused_total`, `last_build_failure`, `last_build_failure_at`, `draining` |

These keys are taken: a factory's own manifest cannot use them. The live log
tail works like any module's: pass `module.Config.Logs` (a `logstream.Hub`
the factory's logger writes to, see module-core `pkg/logging`); it needs no
management of the factory's own.

```go
var level slog.LevelVar
logs := logstream.New(instance)
logger, _ := logging.New("json", os.Stderr, &level, logs)
slog.SetDefault(logger)

own, _ := mgmt.New(manifest) // the factory's own properties, actions, status
own.OnApply(func(ctx context.Context, live map[string]any) error { /* log_level, ... */ return nil })

err := factory.Run(ctx, factory.Config{
	Config: module.Config{AMQPURL: amqpURL, Identity: id, Management: own, Logs: logs},
	Build: &schema, Builder: builder, MaxConcurrentBuilds: 4,
})
```

## Development

```bash
go build ./...
go test ./...
go vet ./...
```

The AMQP integration test is opt-in: it needs a RabbitMQ vhost no real core
consumes (see `pkg/factory/integration_test.go`):

```bash
INTEGRATION_AMQP_URL=amqp://logos:changeme_rabbitmq@localhost:5672/logos-it go test -run Integration ./...
```

Go 1.25.7.
