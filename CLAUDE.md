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

The integration test runs a factory against a scripted core on a real RabbitMQ. It needs a vhost no real core consumes, since the scripted core takes `logos.core.rpc` (setup in the test's comment). No Makefile; Go 1.25.7. CI (`.github/workflows/ci.yml`, the same file in module-core, channel-core, factory-core and the HTTP, TCP and SSH modules; the protocol's adds 32-bit builds) runs `go test ./...` and golangci-lint v2.13.2 with its default linters (no config file) on pushes to `main`, on `v*` tags and on pull requests. The code is kept at 0 issues: fix a finding (an explicit `_ =` with the reason, for a Close whose error says nothing) rather than excluding it.

## Package `pkg/factory`

- **`factory.go`**: `Run(ctx, Config)` is `module.Run` with module type `factory` (defaulted; any other is refused). It validates the schema, declares it under the `build` register field, serves `factory.build` and `factory.build.cancel` as `module.Config.Handlers`, and calls core through a `lifecycle.CoreClient`. The module session outlives `ctx`: builds cancelled at shutdown report before the factory deregisters. `Builder`, `Planner` (a build's plan; `DefaultSteps` without one), `Result`, `Fail`. `once.go`: `BuildOnce` (in process, for builder tests) returns the plan, the progress and the result; `pkg/factorytest` wraps it (`Run`, `Build`).
- **`builds.go`**: the build manager behind the handlers. It validates requests, enforces the build limit and a drain (`busy`), asks a `Planner` builder for the build's plan and sends it with the acceptance (an invalid plan or a builder that does not plan: `DefaultSteps`), remembers builds by id (a repeat is accepted with the same plan, never rebuilt; a finished one resends its result), runs the builder with a context ending at the deadline, sends progress (best effort; `not_found` from core stops the build) and the result (retried only when the call was lost; any reply from core ends it).
- **`manage.go`**: the builds section Run adds to every factory's management (`mgmt.Server.Extend` on the factory's own server, or a server of just the section): `max_concurrent_builds` (live), `drain`/`resume`, build counters, the last failure and `draining`. Its keys are taken; a factory's own manifest using one makes Run fail. Logging and the live log tail stay the factory's (`module.Config.Logs`).
- **`job.go`**: what a builder gets: the request (`Job.Request`), its `MinionConfig` among it for a schema that declares `minion_config` (the build's settings as numbered CBOR, core's convenience: the builder decides what of it goes into the minion, the bytes whole, parts of them or none; `protocol.ValidateBuildRequest` has checked it is there, reads and is this build's). `HasCommand` says whether the request names a command (the operator's picks, the target's required ones); the minion gets those and no others. `Step` reports the plan step the build is on and never blocks (a newer report replaces an unsent one); a step outside the plan is dropped and logged once. The build log (`log.go`) is sent live (`factory.build.log`): written output is split into lines, each with the step it was written in (`Step` moves it; the plan's first before any) and its time, queued (4 MiB at most, the oldest dropped and counted) and sent by the manager's `sendLog` in numbered chunks, one at a time, a lost one resent with its number; `not_found` stops the build, any other refusal stops the live log only. The build's last lines go before the result (`logFlush` at most). Its last 64 KiB also go with the result, as valid UTF-8. `File`/`ReadFile`/`Download` check size and SHA-256. `UploadArtifact` spools to a temp file to send a known length (an empty one as `http.NoBody`), enforces `max_size`, and validates the artifact before uploading. A request's links (a file's `url`, the artifact's `upload_url`) are paths on core's address (`protocol.IsLink`): `Job.link` puts them on `Config.CoreURL`, which Run requires; an absolute URL goes as it is (`factorytest.Store`'s, `BuildOnce` has no core). Transfers use `Config.HTTPClient`, by default module-core's `coretls.Client()`.

## Package `pkg/codeclink`

Given no `HTTPClient`, the link dials with module-core's `coretls.Client()`: core's certificate trusted as `LOGOS_CORE_CA_FILE`, `LOGOS_CORE_CERT_SHA256` or `LOGOS_CORE_TLS_INSECURE` say.

The factory link (docs: contracts/minion-messages/codecs, The factory link), for a factory whose schema names a suite or an envelope of its own (`protocol.BuildSchema.Translates`). `Run(ctx, Options)` keeps the WebSocket to `GET /api/factory/link` open with `Logos-Factory-Instance` (coder/websocket, as channel-core's `linkclient`): reopens with backoff (1 s doubling to 30 s, reset after a minute up; a 403 until registration logs at debug), pings every 30 s, answers each call in its own goroutine within its deadline, `MaxCalls` (64) at once and a failure past it. `Answer(ctx, codec, call)` is one call's answer, validated against its call (`ValidateFor`): a codec's error becomes a failure (`NotOpened`, `NotMessage`, `Refused`, else `other`; the message cut to 512 bytes, UTF-8), and so does a result the call does not take. `factory.Run` runs it with `Config.Codec` and `Config.CoreURL` (a `Codec` required when the schema translates, refused when it does not; `CoreURL` always required; codec names must be the module's), stopping it with ctx. `pkg/factorytest/core.go`: `Core`, core's end of a real link for tests (`NewCore`, `Refuse`, `WaitLinked`, `Drop`, `Call`, `Links`).

## Conventions

- A build result never goes out invalid: the manager validates it and reports `invalid_result` instead.
- Key material, secret values and files never go to logs, progress or the build log.
- Tests: `builds_test.go` drives the manager through its handlers with a fake core and an `httptest` server standing in for core's transfer endpoints; check that a new test fails when the behaviour it covers is broken.
