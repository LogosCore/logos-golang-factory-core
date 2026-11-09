# logos-golang-factory-core

Go SDK for Logos factory modules: the modules that build minions. It builds on
[`logos-golang-module-core`](https://github.com/logoscore/logos-golang-module-core)
(registration, liveness, management, logs) and adds what a factory does: it
declares a build schema at registration and serves builds.

The contract is the Minion Build page of the docs repo
(`contracts/minion-build.md`).

## Usage

A factory declares its build schema and implements `Builder`, and `Planner`
to name the steps of each build:

```go
type builder struct{}

// Steps is the plan for a build of req: core shows it as the build's pipeline.
func (builder) Steps(req protocol.BuildRequest) []protocol.BuildStep {
	return []protocol.BuildStep{{ID: "render", Label: "Render"}, {ID: "upload", Label: "Upload"}}
}

func (builder) Build(ctx context.Context, job *factory.Job) (factory.Result, error) {
	job.Step("render", "writing the minion")
	minion := render(job.Request) // values, commands, connections, files
	job.Step("upload", "")
	if err := job.UploadArtifact(ctx, "minion.json", "application/json", minion); err != nil {
		return factory.Result{}, err
	}
	return factory.Result{}, nil
}

schema := protocol.BuildSchema{ /* targets, fields, channels, commands */ }
err := factory.Run(ctx, factory.Config{
	Config: module.Config{
		AMQPURL:  amqpURL,
		Identity: lifecycle.Identity{ModuleName: "bob", Instance: "bob-1", Version: version},
	},
	Build:   &schema,
	Builder: builder{},
})
```

A builder that does not plan (a `BuilderFunc`, say) has one step,
`factory.DefaultStep` (`build`).

## What Run does for a build

- Checks each `factory.build` against the schema (`protocol.ValidateBuildRequest`)
  and replies at once: accepted with the build's plan (`Planner.Steps`; an
  invalid plan is logged and `DefaultSteps` sent instead), `validation_failed`,
  `unsupported_version` (another schema version), or `busy` (the build limit
  already running, the instance drained, or the factory stopping).
- Answers a repeated build id with the same plan,
  without building twice, and sends a finished build's result again.
- Runs `Builder.Build` with a context that ends at the build's deadline, when
  core cancels it (`factory.build.cancel`, or `not_found` on progress), or when
  the factory stops.
- Sends `factory.build.result` to core until core replies or the deadline
  passes. A builder error becomes a failed result: `Fail(code, message)` for a
  code of the factory's own, otherwise `build_failed`; the SDK's own codes are
  `no_artifact`, `invalid_result`, `timeout`, `shutdown` and `panic`.
- At shutdown it cancels running builds, gives them `ShutdownGrace` to report,
  then deregisters.

The `Job` a builder gets carries the request and target, the plan (`Steps`)
and `Step` to report the step it is on (best effort, never blocks; a step
outside the plan is dropped and logged once), a build log (`Log`, `Logf`; its
last 64 KiB go with the result), `File`, `ReadFile` and `Download` (checked by
size and SHA-256), and `UploadArtifact` (computes the size and SHA-256,
enforces `max_size`).

The schema's `commands` are what the minions can do. The operator picks a
build's, the target's required ones among them, and the request carries them
(`Request.Commands`, `HasCommand`): the builder builds those in and no others,
or fails the build when it cannot.

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

## Codecs

A factory whose minions cannot speak core's codec (the CBOR envelope, the
`aes-256-cbc-hmac-sha256` suite) names a suite or an envelope of its own in
its build schema, `<module_name>.<name>`, with a `codec` declaration, and
translates that layer for core (docs: contracts/minion-messages/codecs). Run
does the rest when it gets a `Codec` and core's base URL: it keeps the
factory link (`pkg/codeclink`) open, reopening it as it drops (refused until
the instance is registered), and answers core's calls with the codec's three
methods, each within the call's deadline:

| Method | Core asks |
|---|---|
| `Inbound` | read a minion's message: open it with one of the candidate keys (the factory's suite), decode it (its envelope); say which key opened it |
| `Outbound` | write core's message: encode it (its envelope), seal it with the id's key (its suite) |
| `Keys` | make the suite's keys for a build, or for a stage (wrapped for the stage item's key; `Refused` for a suite without staging) |

`NotOpened`, `NotMessage` and `Refused` are the failures core tells apart;
any other error goes as `other`, and so does a result the call does not take
(a key index past the candidates, a stage's keys without the wrapped ones).

```go
schema.Crypto = "acme.xchacha"
schema.Codec = &protocol.BuildCodec{Growth: 120, Overhead: 64, Timeout: "2s"}

err := factory.Run(ctx, factory.Config{
	Config: module.Config{AMQPURL: amqpURL, Identity: id},
	Build: &schema, Builder: builder,
	Codec: acmeCodec{}, CoreURL: "http://logos-core:8080",
})
```

Run refuses a schema that names a codec of its own without a `Codec` (and the
other way round), one without `CoreURL`, and names of another module. For
tests, `codeclink.Answer` runs one call in process, and
`factorytest.NewCore` is core's end of a real link: it takes the instance's
link (403 while `Refuse`d) and sends it calls (`Call`), checked as core
checks them.

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
