# Changelog

## v0.4.1

- Requires `logos-golang-protocol` v0.50.0 and `logos-golang-module-core`
  v0.3.2. A build request may carry a minion config
  (`Job.Request.MinionConfig`), checked with the rest of the request: the
  builder decides what of it goes into its minion. Build schemas number
  every channel, field and connection parameter, the tests' too.

## v0.4.0

- **Build files and artifacts go through core.** A `factory.build`'s links
  are paths on core's address (protocol `IsLink`: core streams the files
  and takes the artifact, its storage never reached by a factory), so a
  factory works wherever it reaches core. `Config.CoreURL` is required
  (`LOGOS_CORE_BASE_URL`); transfers use module-core's `coretls.Client()`
  when given no `HTTPClient`, as the factory link does. Requires
  `logos-golang-protocol` v0.49.0 and `logos-golang-module-core` v0.3.1.

## v0.3.1

- The factory link dials core with module-core's `coretls.Client()` (v0.3.0) when given no `HTTPClient`: core over HTTPS with a self-signed certificate (`LOGOS_CORE_CA_FILE`, `LOGOS_CORE_CERT_SHA256`, `LOGOS_CORE_TLS_INSECURE`).

## v0.3.0

- **Codecs** (docs: contracts/minion-messages/codecs): `pkg/codeclink` keeps
  the factory link for a factory that translates its minions' suite or
  envelope; a factory gives `factory.Config` a `Codec` (`Inbound`,
  `Outbound`, `Keys`) and `CoreURL`, and Run keeps the link. `codeclink.Answer`
  and `factorytest.Core` for tests. Requires `logos-golang-protocol` v0.48.1.

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
