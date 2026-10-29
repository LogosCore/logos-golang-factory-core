# Changelog

## v0.2.0

- **The minion id is now the build id**: build operations carry `build_id` (`BuildRequest.BuildID` and the rest), and the SDK names builds by it. Requires `logos-golang-protocol` v0.7.0 and `logos-golang-module-core` v0.2.1.

## v0.1.0

First version: the factory side of the Minion Build contract.

- `factory.Run` runs a factory on module-core's session: declares the build
  schema at registration and serves `factory.build` and
  `factory.build.cancel`.
- `Builder` / `BuilderFunc` build one minion from a `Job`: the request,
  progress, a build log, file downloads checked by SHA-256, and the artifact
  upload.
- Builds are validated against the schema, limited by `MaxConcurrentBuilds`,
  idempotent by minion id (the id of the build and of its minion),
  cancellable, bounded by their deadline, and their results are delivered to
  core until acknowledged. Running builds report `shutdown` before the factory
  deregisters.
- Every factory is managed for its builds: Run adds a builds section to the
  factory's own management, or makes one of just that section
  (`max_concurrent_builds`, live; `drain` and `resume`, each available only
  when it changes something; build counters, the last failure and
  `draining`). `module.Config.Logs` works with or without
  management of the factory's own.
- Requires `logos-golang-module-core` v0.2.0 and `logos-golang-protocol`
  v0.6.0.
