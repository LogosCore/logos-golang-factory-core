# Changelog

## v0.1.0 (unreleased)

First version: the factory side of the Minion Build contract.

- `factory.Run` runs a factory on module-core's session: declares the build
  schema at registration and serves `factory.build` and
  `factory.build.cancel`.
- `Builder` / `BuilderFunc` build one minion from a `Job`: the request,
  progress, a build log, file downloads checked by SHA-256, and the artifact
  upload.
- Builds are validated against the schema, limited by `MaxConcurrentBuilds`,
  idempotent by build id, cancellable, bounded by their deadline, and their
  results are delivered to core until acknowledged. Running builds report
  `shutdown` before the factory deregisters.
- Requires `logos-golang-module-core` v0.2.0 and `logos-golang-protocol`
  v0.6.0.
