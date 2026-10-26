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
INTEGRATION_AMQP_URL=amqp://logos:changeme_rabbitmq@localhost:5672/logos-it go test -run Integration ./...
```

The integration test runs a factory against a scripted core on a real RabbitMQ. It needs a vhost no real core consumes, since the scripted core takes `logos.core.rpc` (setup in the test's comment). No Makefile; Go 1.25.7.

## Package `pkg/factory`

- **`factory.go`**: `Run(ctx, Config)` is `module.Run` with module type `factory` (defaulted; any other is refused). It validates the schema, declares it under the `build` register field, serves `factory.build` and `factory.build.cancel` as `module.Config.Handlers`, and calls core through a `lifecycle.CoreClient`. The module session outlives `ctx`: builds cancelled at shutdown report before the factory deregisters. `Builder`, `Result`, `Fail`.
- **`builds.go`**: the build manager behind the handlers. It validates requests, enforces `MaxConcurrentBuilds` (`busy`), remembers builds by id (a repeat is accepted, never rebuilt; a finished one resends its result), runs the builder with a context ending at the deadline, sends progress (best effort; `not_found` from core stops the build) and the result (retried only when the call was lost; any reply from core ends it).
- **`job.go`**: what a builder gets. `Progress` never blocks (a newer step replaces an unsent one). The build log keeps the last 64 KiB as valid UTF-8. `File`/`ReadFile`/`Download` check size and SHA-256. `UploadArtifact` spools to a temp file to send a known length (an empty one as `http.NoBody`), enforces `max_size`, and validates the artifact before uploading.

## Conventions

- A build result never goes out invalid: the manager validates it and reports `invalid_result` instead.
- Key material, secret values and files never go to logs, progress or the build log.
- Tests: `builds_test.go` drives the manager through its handlers with a fake core and an `httptest` object store; check that a new test fails when the behaviour it covers is broken.
