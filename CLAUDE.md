# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go SDK (`github.com/logoscore/logos-golang-factory-core`) for Logos factory modules, which build minions. It builds on `github.com/logoscore/logos-golang-module-core` (the control plane every module type shares) and adds only what is factory-specific: the build schema declared at registration and the build operations. Depends on `github.com/logoscore/logos-golang-protocol` for the schema and payload types and their validation.

The contract is `contracts/minion-build.md` in the docs repo (`logos-docs`).

## Build & Test

```bash
go build ./...
go test ./...
go vet ./...
golangci-lint run ./...
INTEGRATION_AMQP_URL=amqp://logos:changeme_rabbitmq@localhost:5672/logos-it go test -run Integration ./...
```

The integration test runs a factory against a scripted core on a real RabbitMQ. It needs a vhost no real core consumes, since the scripted core takes `logos.core.rpc` (setup in the test's comment). No Makefile; Go 1.25.7. CI (`.github/workflows/ci.yml`, the same file in protocol, module-core, channel-core, factory-core and the HTTP channel) runs `go test ./...` and golangci-lint v2.13.2 with its default linters (no config file) on pushes to `main`, on `v*` tags and on pull requests. The code is kept at 0 issues: fix a finding (an explicit `_ =` with the reason, for a Close whose error says nothing) rather than excluding it.

## Package `pkg/factory`

- **`factory.go`**: `Run(ctx, Config)` is `module.Run` with module type `factory` (defaulted; any other is refused). It validates the schema, declares it under the `build` register field, serves `factory.build` and `factory.build.cancel` as `module.Config.Handlers`, and calls core through a `lifecycle.CoreClient`. The module session outlives `ctx`: builds cancelled at shutdown report before the factory deregisters. `Builder`, `Planner` (a build's plan; `DefaultSteps` without one), `Result`, `Fail`. `once.go`: `BuildOnce` (in process, for builder tests) returns the plan, the progress and the result; `pkg/factorytest` wraps it (`Run`, `Build`).
- **`builds.go`**: the build manager behind the handlers. It validates requests, enforces the build limit and a drain (`busy`), asks a `Planner` builder for the build's plan and sends it with the acceptance (an invalid plan or a builder that does not plan: `DefaultSteps`), remembers builds by id (a repeat is accepted with the same plan, never rebuilt; a finished one resends its result), runs the builder with a context ending at the deadline, sends progress (best effort; `not_found` from core stops the build) and the result (retried only when the call was lost; any reply from core ends it).
- **`manage.go`**: the builds section Run adds to every factory's management (`mgmt.Server.Extend` on the factory's own server, or a server of just the section): `max_concurrent_builds` (live), `drain`/`resume`, build counters, the last failure and `draining`. Its keys are taken; a factory's own manifest using one makes Run fail. Logging and the live log tail stay the factory's (`module.Config.Logs`).
- **`job.go`**: what a builder gets. `HasCommand` says whether the request names a command (the operator's picks, the target's required ones); the minion gets those and no others. `Step` reports the plan step the build is on and never blocks (a newer report replaces an unsent one); a step outside the plan is dropped and logged once. The build log keeps the last 64 KiB as valid UTF-8. `File`/`ReadFile`/`Download` check size and SHA-256. `UploadArtifact` spools to a temp file to send a known length (an empty one as `http.NoBody`), enforces `max_size`, and validates the artifact before uploading.

## Conventions

- A build result never goes out invalid: the manager validates it and reports `invalid_result` instead.
- Key material, secret values and files never go to logs, progress or the build log.
- Tests: `builds_test.go` drives the manager through its handlers with a fake core and an `httptest` object store; check that a new test fails when the behaviour it covers is broken.
